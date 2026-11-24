use sha2::{Digest, Sha256};

#[cfg(test)]
include!("runner_fixture_tests.rs");

const FIXTURE_FRAME_LIMIT: usize = 4 * 1024 * 1024;
const FIXTURE_OUTPUT_LIMIT: usize = 1024 * 1024;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FixtureAuthority {
    pub campaign_id: String,
    pub cell_run_id: String,
    pub owner_id: String,
    pub fencing_token: u64,
    pub namespace: String,
    pub server: String,
    pub context: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureCommandRequest {
    pub schema_version: u32,
    pub kind: String,
    pub request_id: u64,
    pub authority: FixtureAuthority,
    pub original_deadline_unix_ms: u64,
    pub remaining_timeout_ms: u64,
    pub executable: String,
    pub argv: Vec<String>,
    pub current_dir: Option<String>,
    pub stdin: Option<String>,
    pub stdin_sha256: String,
    pub request_sha256: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureCommandResponse {
    pub schema_version: u32,
    pub kind: String,
    pub request_id: u64,
    pub request_sha256: String,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub truncated: bool,
}

#[async_trait::async_trait]
trait FixtureCommandTransport: Send + Sync {
    async fn exchange(&self, request: &FixtureCommandRequest) -> Result<FixtureCommandResponse>;
}

struct FixtureBoundary {
    authority: FixtureAuthority,
    deadline_unix_ms: u64,
    sequence: std::sync::atomic::AtomicU64,
    transport: Arc<dyn FixtureCommandTransport>,
    responses: tokio::sync::Mutex<Vec<FixtureCommandResponse>>,
}

tokio::task_local! {
    static FIXTURE_BOUNDARY: FixtureBoundary;
}

fn fixture_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn fixture_canonical_json(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Object(object) => {
            let sorted = object.iter().collect::<BTreeMap<_, _>>();
            let fields = sorted
                .into_iter()
                .map(|(key, value)| {
                    Ok(format!(
                        "{}:{}",
                        serde_json::to_string(key)?,
                        fixture_canonical_json(value)?
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            format!("{{{}}}", fields.join(","))
        }
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(fixture_canonical_json)
                .collect::<Result<Vec<_>>>()?
                .join(",")
        ),
        _ => serde_json::to_string(value)?,
    })
}

impl FixtureCommandRequest {
    fn digest(&self) -> Result<String> {
        let mut value = serde_json::to_value(self)?;
        value
            .as_object_mut()
            .context("command request is not an object")?
            .remove("request_sha256");
        Ok(fixture_sha256(fixture_canonical_json(&value)?.as_bytes()))
    }

    fn validate_response(&self, response: &FixtureCommandResponse) -> Result<()> {
        if response.schema_version != 1
            || response.kind != "command_response"
            || response.request_id != self.request_id
            || response.request_sha256 != self.request_sha256
            || response.truncated
            || response.stdout.len() > FIXTURE_OUTPUT_LIMIT
            || response.stderr.len() > FIXTURE_OUTPUT_LIMIT
            || unix_time_ms() >= u128::from(self.original_deadline_unix_ms)
        {
            bail!("fixture bridge returned an unbound, late, truncated or oversized response");
        }
        Ok(())
    }
}

async fn fixture_command_output(command: &Command, payload: Option<&[u8]>) -> Result<Option<(i32, String)>> {
    let Ok(boundary) = FIXTURE_BOUNDARY.try_with(|boundary| {
        (
            boundary.authority.clone(),
            boundary.deadline_unix_ms,
            boundary.sequence.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            Arc::clone(&boundary.transport),
        )
    }) else {
        return Ok(None);
    };
    let (authority, deadline, request_id, transport) = boundary;
    let remaining = deadline
        .checked_sub(u64::try_from(unix_time_ms())?)
        .filter(|value| *value > 0)
        .context("fixture original deadline expired before command dispatch")?;
    let command = command.as_std();
    if command.get_envs().next().is_some() {
        bail!("fixture bridge cannot serialize command environment overrides");
    }
    let executable = command
        .get_program()
        .to_str()
        .context("fixture executable is not UTF-8")?
        .to_owned();
    let argv = command
        .get_args()
        .map(|value| value.to_str().map(str::to_owned).context("fixture argv is not UTF-8"))
        .collect::<Result<Vec<_>>>()?;
    let mut request = FixtureCommandRequest {
        schema_version: 1,
        kind: "command_request".to_owned(),
        request_id,
        authority,
        original_deadline_unix_ms: deadline,
        remaining_timeout_ms: remaining,
        executable,
        argv,
        current_dir: command
            .get_current_dir()
            .map(|value| {
                value
                    .to_str()
                    .map(str::to_owned)
                    .context("fixture working directory is not UTF-8")
            })
            .transpose()?,
        stdin: payload
            .map(|value| String::from_utf8(value.to_vec()).context("fixture stdin is not UTF-8"))
            .transpose()?,
        stdin_sha256: fixture_sha256(payload.unwrap_or_default()),
        request_sha256: String::new(),
    };
    request.request_sha256 = request.digest()?;
    if serde_json::to_vec(&request)?.len() >= FIXTURE_FRAME_LIMIT {
        bail!("fixture bridge command exceeds the bounded frame size");
    }
    let response = tokio::time::timeout(Duration::from_millis(remaining), transport.exchange(&request))
        .await
        .context("fixture bridge exceeded the original deadline; outcome unknown, do not replay")??;
    request.validate_response(&response)?;
    let output = (response.exit_code, response.stdout.clone());
    FIXTURE_BOUNDARY.with(|boundary| -> Result<()> {
        boundary
            .responses
            .try_lock()
            .context("fixture bridge commands must be sequential")?
            .push(response);
        Ok(())
    })?;
    Ok(Some(output))
}

struct StdioFixtureTransport {
    input: tokio::sync::Mutex<BufReader<tokio::io::Stdin>>,
}

#[async_trait::async_trait]
impl FixtureCommandTransport for StdioFixtureTransport {
    async fn exchange(&self, request: &FixtureCommandRequest) -> Result<FixtureCommandResponse> {
        let mut input = self
            .input
            .try_lock()
            .context("fixture bridge commands must be sequential")?;
        let mut output = tokio::io::stdout();
        let mut frame = serde_json::to_vec(request)?;
        frame.push(b'\n');
        output.write_all(&frame).await?;
        output.flush().await?;
        let mut bytes = Vec::new();
        (&mut *input)
            .take((FIXTURE_FRAME_LIMIT + 1) as u64)
            .read_until(b'\n', &mut bytes)
            .await?;
        if bytes.len() > FIXTURE_FRAME_LIMIT || bytes.last() != Some(&b'\n') {
            bail!("fixture bridge missing or response frame truncated/oversized");
        }
        serde_json::from_slice(&bytes).context("parse fixture command response")
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureSetupInput {
    pub schema_version: u32,
    pub authority: FixtureAuthority,
    pub original_deadline_unix_ms: u64,
    pub inventory: RunInventory,
    pub service_account: String,
    pub manifests: Vec<Value>,
    pub runtime_directory: PathBuf,
    pub stamp_config_path: PathBuf,
    pub bootstrap_env_file: PathBuf,
}

fn fixture_deployment(namespace: &str, role: &str, service_account: &str, image: &str) -> Value {
    let name = match role {
        "maintenance" => "delta-maintenance".to_owned(),
        _ => format!("arrowcol-{role}"),
    };
    let app = if role == "client" {
        "arrowcol-oat-client".to_owned()
    } else {
        name.clone()
    };
    json!({
        "apiVersion":"apps/v1","kind":"Deployment",
        "metadata":{"name":name,"namespace":namespace,"labels":{"app":app}},
        "spec":{"replicas":1,"selector":{"matchLabels":{"app":app}},
            "template":{"metadata":{"labels":{"app":app}},
                "spec":{"serviceAccountName":service_account,"automountServiceAccountToken":false,
                    "containers":[{"name":role,"image":image,
                        "resources":{"requests":{"cpu":"10m","memory":"16Mi"},
                            "limits":{"cpu":"100m","memory":"64Mi"}}}]}}}
    })
}

impl FixtureSetupInput {
    fn validate(&self) -> Result<()> {
        self.inventory.validate()?;
        let inventory = &self.inventory;
        let authority = &self.authority;
        let remaining = self
            .original_deadline_unix_ms
            .checked_sub(u64::try_from(unix_time_ms())?)
            .context("fixture original deadline has expired")?;
        if self.schema_version != 1
            || remaining == 0
            || remaining > 600_000
            || inventory.load_admission != InventoryLoadAdmission::NotAuthorized
            || inventory.planned_cell_duration_ms != Some(0)
            || inventory.namespace.uid.is_some()
            || !inventory.resources.is_empty()
            || authority.campaign_id != inventory.campaign_id
            || authority.cell_run_id != inventory.cell_run_id
            || authority.owner_id != inventory.owner_id
            || authority.fencing_token != inventory.fencing_token
            || authority.namespace != inventory.namespace.name
        {
            bail!(
                "fixture requires fresh exact ownership, original bounded deadline and zero-row not_authorized inventory"
            );
        }
        let plan = inventory
            .fic
            .as_ref()
            .and_then(|fic| fic.plan.as_ref())
            .context("fixture requires the reviewed FIC/stamp authority plan")?;
        if authority.server != plan.server
            || authority.context.is_empty()
            || authority.context.len() > 120
            || authority.context.bytes().any(|byte| byte.is_ascii_control())
            || self.service_account != plan.service_account
            || plan.run_id != inventory.cell_run_id
            || plan.namespace != inventory.namespace.name
        {
            bail!("fixture server/context/service-account differs from reviewed authority");
        }
        let server = Url::parse(&authority.server)?;
        if server.scheme() != "https"
            || server.host_str().is_none()
            || !server.username().is_empty()
            || server.password().is_some()
            || server.query().is_some()
            || server.fragment().is_some()
        {
            bail!("fixture API server must be a credential-free HTTPS authority");
        }
        if !self.runtime_directory.is_absolute()
            || !self.stamp_config_path.is_absolute()
            || !self.bootstrap_env_file.is_absolute()
        {
            bail!("fixture paths must be explicit absolute paths");
        }
        let namespace = &inventory.namespace.name;
        let namespace_manifest = json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":namespace}});
        let service_account = json!({"apiVersion":"v1","kind":"ServiceAccount",
            "metadata":{"name":self.service_account,"namespace":namespace},
            "automountServiceAccountToken":false});
        if self.manifests.len() != 5 || self.manifests[0] != namespace_manifest || self.manifests[1] != service_account
        {
            bail!("fixture requires exactly the reviewed Namespace, ServiceAccount and three role Deployments");
        }
        for (resource, role) in self.manifests[2..].iter().zip(["client", "collector", "maintenance"]) {
            let image = resource
                .pointer("/spec/template/spec/containers/0/image")
                .and_then(Value::as_str)
                .context("fixture role image is missing")?;
            let digest = image
                .strip_prefix("registry.k8s.io/pause@sha256:")
                .context("fixture roles require pinned no-data pause images")?;
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                || *resource != fixture_deployment(namespace, role, &self.service_account, image)
            {
                bail!("fixture role Deployment is not the exact bounded no-data manifest");
            }
        }
        Ok(())
    }
}

fn fixture_response_identity(resource: &Value, expected: &Value) -> Result<()> {
    for pointer in ["/apiVersion", "/kind", "/metadata/name", "/metadata/namespace"] {
        if resource.pointer(pointer) != expected.pointer(pointer) {
            bail!("fresh-create response differs from the exact requested identity");
        }
    }
    for field in ["uid", "resourceVersion"] {
        if resource["metadata"][field].as_str().is_none_or(str::is_empty) {
            bail!("fresh-create response omitted {field}");
        }
    }
    if resource["kind"] == "Deployment"
        && resource["metadata"]["generation"]
            .as_u64()
            .is_none_or(|value| value == 0)
    {
        bail!("fresh-create Deployment response omitted generation");
    }
    let run_id = expected
        .pointer("/metadata/labels/arrowcol-oat~1run-id")
        .and_then(Value::as_str)
        .context("requested resource has no run origin")?;
    let owner = expected
        .pointer("/metadata/labels/arrowcol-oat~1owner-token")
        .and_then(Value::as_str)
        .context("requested resource has no owner origin")?;
    if !has_origin_markers(&resource["metadata"], run_id, owner)
        || (resource["kind"] == "Deployment"
            && !resource
                .pointer("/spec/template/metadata")
                .is_some_and(|metadata| has_origin_markers(metadata, run_id, owner)))
    {
        bail!("fresh-create response changed origin markers");
    }
    Ok(())
}

fn parse_fresh_create_responses(output: &str, expected: &[Value]) -> Result<Vec<Value>> {
    // kubectl prints each created object separately; a failed batch may already have created a subset.
    let responses = serde_json::Deserializer::from_str(output)
        .into_iter::<Value>()
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("fresh-create returned malformed/truncated JSON; creation outcome unknown, do not replay")?;
    if responses.len() != expected.len() {
        bail!("fresh-create did not return every requested identity; creation outcome unknown, do not replay");
    }
    let mut identities = BTreeSet::new();
    let mut uids = BTreeSet::new();
    for actual in &responses {
        let requested = expected
            .iter()
            .find(|requested| {
                ["/apiVersion", "/kind", "/metadata/namespace", "/metadata/name"]
                    .iter()
                    .all(|path| actual.pointer(path) == requested.pointer(path))
            })
            .context("fresh-create returned an unexpected resource identity; do not adopt or replay")?;
        fixture_response_identity(actual, requested)?;
        let identity = (
            actual["apiVersion"].as_str(),
            actual["kind"].as_str(),
            actual["metadata"]["namespace"].as_str(),
            actual["metadata"]["name"].as_str(),
        );
        if !identities.insert(identity) || !uids.insert(actual["metadata"]["uid"].as_str()) {
            bail!("fresh-create returned duplicate resource identity or UID; do not adopt or replay");
        }
    }
    Ok(responses)
}

async fn verify_fresh_namespace_readback(config: &OatConfig, platform: &AksPlatform) -> Result<()> {
    let output = kubectl(
        config,
        vec![
            "get",
            "namespace",
            &config.namespace,
            "--ignore-not-found",
            "-o",
            "json",
        ],
        None,
    )
    .await
    .context("reobserve the original fresh Namespace before setup publication")?;
    if output.trim().is_empty() {
        bail!("fresh Namespace is absent at setup readback; retaining planned custody");
    }
    let observed: Value = serde_json::from_str(&output).context("parse fresh Namespace readback")?;
    let mut expected = json!({"apiVersion":"v1","kind":"Namespace","metadata":{"name":config.namespace}});
    set_origin_markers(
        &mut expected["metadata"],
        &platform.run_id,
        &platform.namespace_owner_token,
    )?;
    fixture_response_identity(&observed, &expected)?;
    if observed["metadata"]["uid"].as_str() != platform.namespace_uid.as_deref() {
        bail!("fresh Namespace UID changed after creation; retaining planned custody");
    }
    // This is an observation, not an atomic Namespace-UID precondition on child creation.
    Ok(())
}

async fn fixture_setup_with_transport(
    input: FixtureSetupInput,
    transport: Option<Arc<dyn FixtureCommandTransport>>,
) -> Result<Value> {
    input.validate()?;
    let transport = transport.context("fixture command bridge is required; direct process dispatch is forbidden")?;
    let boundary = FixtureBoundary {
        authority: input.authority.clone(),
        deadline_unix_ms: input.original_deadline_unix_ms,
        sequence: std::sync::atomic::AtomicU64::new(1),
        transport,
        responses: tokio::sync::Mutex::new(Vec::new()),
    };
    FIXTURE_BOUNDARY
        .scope(boundary, async move {
            let mut config: OatConfig = serde_json::from_str(include_str!(
                "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
            ))?;
            config.platform.kind = crate::config::OatMode::Aks;
            config.campaign_profile = Some(crate::config::CampaignProfile::SingleHotTable);
            config.namespace = input.authority.namespace.clone();
            config.kubectl_context = Some(input.authority.context.clone());
            let mut platform = fixture_platform(&input);
            tokio::fs::create_dir(&input.runtime_directory)
                .await
                .context("create fresh fixture evidence directory")?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                tokio::fs::set_permissions(&input.runtime_directory, fs::Permissions::from_mode(0o700)).await?;
            }
            persist_campaign_inventory(&platform, &input.inventory).await?;
            let collision = kubectl(
                &config,
                vec![
                    "get",
                    "namespace",
                    &config.namespace,
                    "--ignore-not-found",
                    "-o",
                    "json",
                ],
                None,
            )
            .await?;
            if !collision.trim().is_empty() {
                bail!("fixture Namespace already exists; fresh-create cannot adopt it");
            }
            let mut namespace = input.manifests[0].clone();
            set_origin_markers(
                &mut namespace["metadata"],
                &platform.run_id,
                &platform.namespace_owner_token,
            )?;
            let output = kubectl(
                &config,
                vec!["create", "-f", "-", "-o", "json"],
                Some(serde_json::to_vec(&namespace)?),
            )
            .await?;
            let actual: Value = serde_json::from_str(&output)?;
            fixture_response_identity(&actual, &namespace)?;
            platform.namespace_uid = Some(
                actual["metadata"]["uid"]
                    .as_str()
                    .context("namespace UID missing")?
                    .to_owned(),
            );
            record_campaign_namespace_uid(&config, &platform).await?;
            apply_runtime_resources_with_mode(
                &config,
                &mut platform,
                input.manifests[1..].to_vec(),
                RuntimeResourceMode::FreshCreate,
            )
            .await?;
            let inventory: RunInventory = serde_json::from_slice(
                &tokio::fs::read(input.runtime_directory.join("campaign-inventory.json")).await?,
            )?;
            validate_campaign_inventory_identity(
                &inventory,
                &input.authority.campaign_id,
                &input.authority.owner_id,
                input.authority.fencing_token,
                &input.inventory.orchestration_run_id,
                &platform.run_id,
            )?;
            if inventory.namespace.uid != platform.namespace_uid {
                bail!("fixture inventory no longer matches the original created namespace UID");
            }
            if inventory.load_admission != InventoryLoadAdmission::NotAuthorized {
                bail!("fixture setup unexpectedly admitted load");
            }
            let responses = FIXTURE_BOUNDARY.with(|boundary| {
                boundary
                    .responses
                    .try_lock()
                    .map(|responses| responses.clone())
                    .context("fixture command still in flight at receipt")
            })?;
            Ok(
                json!({"schema_version":1,"kind":"fixture_setup_receipt","inventory":inventory,
            "namespace_response":actual,"command_responses":responses,"load_admitted":false}),
            )
        })
        .await
}

fn fixture_platform(input: &FixtureSetupInput) -> AksPlatform {
    AksPlatform {
        run_id: input.inventory.cell_run_id.clone(),
        resource_readback: Vec::new(),
        cpu_throttling_start: None,
        cpu_throttling_evidence: None,
        namespace_created: false,
        namespace_create_attempted: false,
        namespace_owner_token: input.inventory.owner_id.clone(),
        namespace_uid: None,
        runtime_dir: Some(input.runtime_directory.clone()),
        qap_relative_data_root: None,
        delta_root: None,
        router_metrics_url: None,
        router_tunnel: None,
        fic_record_path: None,
        fic_ready_snapshot_path: None,
        fic_prepare_attempted: false,
        qap_record_path: None,
        qap_bind_attempted: false,
        qap_cleanup_confirmed: false,
        qap_outcome_unresolved: false,
        run_table_initialization_attempted: false,
        run_table_initialization_drained: false,
        workload_stop_record_path: None,
        schedule_configmap_name: String::new(),
        service_account: Some(input.service_account.clone()),
        router_yaml: None,
        shutdown_drain_evidence: None,
        campaign_cli_paths: Some(OwnedAksCampaignConfigPaths {
            stamp_config_path: input.stamp_config_path.clone(),
            bootstrap_env_file: input.bootstrap_env_file.clone(),
        }),
        maintenance_cell_baseline: Vec::new(),
        stopped_collector_logs: None,
        custody_followers: Vec::new(),
        shutdown_membership: None,
    }
}

/// Run only the reviewed zero-row setup through an explicit command bridge.
///
/// # Errors
/// Refuses malformed inputs, missing bridge, collisions and unbound responses.
pub async fn run_fixture_setup(path: &Path, bridge_stdio: bool) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(path).await?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > FIXTURE_OUTPUT_LIMIT as u64 {
        bail!("fixture input must be a bounded regular non-symlink file");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("fixture input must be private to its owner");
        }
    }
    let input: FixtureSetupInput = serde_json::from_slice(&tokio::fs::read(path).await?)?;
    let transport = bridge_stdio.then(|| {
        Arc::new(StdioFixtureTransport {
            input: tokio::sync::Mutex::new(BufReader::new(tokio::io::stdin())),
        }) as Arc<dyn FixtureCommandTransport>
    });
    let receipt = fixture_setup_with_transport(input, transport).await?;
    let mut bytes = serde_json::to_vec(&receipt)?;
    if bytes.len() >= FIXTURE_FRAME_LIMIT {
        bail!("fixture receipt exceeds frame limit; retained inventory remains authoritative");
    }
    bytes.push(b'\n');
    let mut output = tokio::io::stdout();
    output.write_all(&bytes).await?;
    output.flush().await?;
    Ok(())
}

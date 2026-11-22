#[allow(dead_code)]
mod fixture {
    include!("darwin_maintenance.rs");

    fn checked_files(root: &Path) -> Result<BTreeMap<String, String>> {
        let files = active_files(root)?;
        let root = fs::canonicalize(root)?;
        for path in files.keys() {
            let file = data_path(&root, path)?;
            let resolved = fs::canonicalize(&file)?;
            ensure!(resolved.starts_with(&root), "active file escapes table");
            ensure!(fs::symlink_metadata(&file)?.file_type().is_file(), "active path is not a regular file");
        }
        Ok(files)
    }

    fn checked_rows(root: &Path) -> Result<(Vec<Row>, u64, usize)> {
        checked_files(root)?;
        rows_and_bytes(root)
    }

    fn log_version(root: &Path) -> Result<u64> {
        let mut versions = Vec::new();
        for entry in fs::read_dir(root.join("_delta_log"))? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                let stem = path.file_stem().and_then(|s| s.to_str()).context("invalid log name")?;
                ensure!(stem.len() == 20 && stem.bytes().all(|c| c.is_ascii_digit()), "invalid log version");
                versions.push(stem.parse::<u64>()?);
            }
        }
        versions.sort_unstable();
        ensure!(
            versions.iter().enumerate().all(|(index, version)| *version == index as u64),
            "Delta log versions are not contiguous"
        );
        versions.last().copied().context("missing Delta log")
    }

    fn original_logs_unchanged(before: &Path, after: &Path) -> Result<u64> {
        let version = log_version(before)?;
        ensure!(log_version(after)? >= version, "Delta log went backwards");
        for number in 0..=version {
            let name = format!("{number:020}.json");
            ensure!(
                fs::read(before.join("_delta_log").join(&name))?
                    == fs::read(after.join("_delta_log").join(&name))?,
                "original Delta log changed"
            );
        }
        Ok(version)
    }

    fn active(seed: &Path, stopped: &Path) -> Result<Value> {
        let before = seed.join("active");
        let after = stopped.join("active");
        let (before_rows, before_bytes, before_count) = checked_rows(&before)?;
        let (after_rows, after_bytes, after_count) = checked_rows(&after)?;
        ensure!(before_rows.len() == 34 * ROWS_PER_FILE as usize && before_count == 34, "unexpected seed");
        for (index, (id, name, part)) in before_rows.iter().enumerate() {
            let id_expected = index as i64;
            let file = id_expected / ROWS_PER_FILE;
            let part_expected = if file < 16 { 0 } else if file < 32 { 1 } else { 2 };
            ensure!(
                *id == id_expected
                    && *name == (id_expected % 3 != 0).then(|| format!("n{id_expected}"))
                    && *part == partition(part_expected),
                "seed rows differ from fixed fixture"
            );
        }
        ensure!(before_rows == after_rows, "active physical rows changed");
        ensure!(metadata(&before)? == metadata(&after)?, "metadata changed");
        let protected_before = checked_files(&before)?.into_iter()
            .filter(|(_, part)| part == &partition(2)).collect::<BTreeMap<_, _>>();
        let protected_after = checked_files(&after)?.into_iter()
            .filter(|(_, part)| part == &partition(2)).collect::<BTreeMap<_, _>>();
        ensure!(protected_before.len() == 2 && protected_before == protected_after, "protected paths changed");
        for path in protected_before.keys() {
            ensure!(fs::read(data_path(&before, path)?)? == fs::read(data_path(&after, path)?)?, "protected file changed");
        }
        let initial_version = original_logs_unchanged(&before, &after)?;
        let final_version = log_version(&after)?;
        ensure!(final_version > initial_version, "active fixture did not compact");
        verify_rewrite_actions(&after, initial_version, final_version)?;
        let table = DeltaTable::open_for_maintenance(&url(&after)?)?;
        let plan = table.prepare_optimize(&optimize())?;
        ensure!(plan.plan.bins.is_empty() && plan.plan.compaction_debt.eligible_files == 0, "eligible debt remains");
        Ok(json!({
            "rows": after_rows.len(), "initial_files": before_count, "initial_bytes": before_bytes,
            "final_files": after_count, "final_bytes": after_bytes,
            "protected_files": protected_after.len(), "rewrite_commits": final_version - initial_version
        }))
    }

    fn idle(seed: &Path, stopped: &Path) -> Result<Value> {
        for name in ["empty", "quiescent", "protected"] {
            let before = seed.join(name);
            let after = stopped.join(name);
            ensure!(original_logs_unchanged(&before, &after)? == log_version(&after)?, "idle Delta commit");
            ensure!(checked_files(&before)? == checked_files(&after)?, "idle active paths changed");
            ensure!(checked_rows(&before)? == checked_rows(&after)?, "idle rows or bytes changed");
            ensure!(metadata(&before)? == metadata(&after)?, "idle metadata changed");
            for path in checked_files(&before)?.keys() {
                ensure!(fs::read(data_path(&before, path)?)? == fs::read(data_path(&after, path)?)?, "idle physical file changed");
            }
        }
        let before = seed.join("due");
        let after = stopped.join("due");
        let initial_version = original_logs_unchanged(&before, &after)?;
        ensure!(log_version(&after)? == initial_version + 1, "due fixture did not append exactly once");
        let (rows, _, _) = checked_rows(&after)?;
        ensure!(rows.len() == 3 * ROWS_PER_FILE as usize, "due fixture rows missing");
        for (index, (id, name, part)) in rows.iter().enumerate() {
            let expected = index as i64;
            let expected_part = if (ROWS_PER_FILE..2 * ROWS_PER_FILE).contains(&expected) { 1 } else { 0 };
            ensure!(
                *id == expected
                    && *name == (expected % 3 != 0).then(|| format!("n{expected}"))
                    && *part == partition(expected_part),
                "due fixture rows differ"
            );
        }
        let table = DeltaTable::open_for_maintenance(&url(&after)?)?;
        ensure!(!table.plan_optimize(&optimize())?.bins.is_empty(), "due compaction debt absent");
        let purge = table.plan_purge(&PurgeConfig::new(
            RETENTION, BASE + 8 * 24 * 60 * MINUTE, Granularity::Minute,
        ))?;
        ensure!(purge.eligible_files > 0, "due retention debt absent");
        Ok(json!({"idle_commits": 0, "due_append_commits": 1, "due_rows": rows.len()}))
    }

    pub fn compare() -> Result<()> {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        ensure!(args.len() == 3, "usage: darwin_maintenance_verify <active|idle> <seed> <stopped>");
        let seed = Path::new(&args[1]);
        let stopped = Path::new(&args[2]);
        let result = match args[0].as_str() {
            "active" => active(seed, stopped)?,
            "idle" => idle(seed, stopped)?,
            _ => bail!("unknown maintenance fixture"),
        };
        println!("{}", result);
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    fixture::compare()
}

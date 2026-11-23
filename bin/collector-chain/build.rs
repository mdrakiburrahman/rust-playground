use std::collections::HashSet;
use std::env;

fn main() {
    const PROFILE_VARIABLES: [&str; 4] = [
        "ARROWCOL_OAT_CLIENT_BUILD_LTO",
        "ARROWCOL_OAT_CLIENT_BUILD_CODEGEN_UNITS",
        "ARROWCOL_OAT_CLIENT_BUILD_OPT_LEVEL",
        "ARROWCOL_OAT_CLIENT_BUILD_TARGET_CPU",
    ];
    for name in PROFILE_VARIABLES {
        println!("cargo:rerun-if-env-changed={name}");
    }
    println!("cargo:rerun-if-env-changed=OPT_LEVEL");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_FEATURE");

    let values = PROFILE_VARIABLES.map(|name| env::var(name).ok());
    if values.iter().all(Option::is_none) {
        return;
    }
    if values.iter().any(Option::is_none) {
        let missing = PROFILE_VARIABLES.iter().zip(values.iter())
            .filter_map(|(name, value)| value.is_none().then_some(*name))
            .collect::<Vec<_>>();
        panic!("client build-profile metadata is incomplete; missing {}", missing.join(", "));
    }
    let [Some(lto), Some(codegen_units), Some(opt_level), Some(target_cpu)] = values else {
        unreachable!();
    };
    if !matches!(lto.as_str(), "off" | "thin-local" | "thin" | "fat") {
        panic!("ARROWCOL_OAT_CLIENT_BUILD_LTO has unsupported value {lto}");
    }
    let codegen_units = codegen_units.parse::<u16>()
        .unwrap_or_else(|_| panic!("ARROWCOL_OAT_CLIENT_BUILD_CODEGEN_UNITS must be an integer in 1..=256"));
    if !(1..=256).contains(&codegen_units) {
        panic!("ARROWCOL_OAT_CLIENT_BUILD_CODEGEN_UNITS must be an integer in 1..=256");
    }
    if !matches!(opt_level.as_str(), "2" | "3") {
        panic!("ARROWCOL_OAT_CLIENT_BUILD_OPT_LEVEL must be 2 or 3");
    }
    let cargo_opt_level = env::var("OPT_LEVEL").unwrap_or_else(|_| panic!("Cargo did not provide OPT_LEVEL"));
    if cargo_opt_level != opt_level {
        panic!("ARROWCOL_OAT_CLIENT_BUILD_OPT_LEVEL={opt_level} does not match Cargo OPT_LEVEL={cargo_opt_level}");
    }

    let features = env::var("CARGO_CFG_TARGET_FEATURE").unwrap_or_default();
    let enabled = features.split(',').filter(|feature| !feature.is_empty()).collect::<HashSet<_>>();
    let required = arrowcol_tunables::required_rust_target_features(&target_cpu)
        .unwrap_or_else(|error| panic!("ARROWCOL_OAT_CLIENT_BUILD_TARGET_CPU: {error}"));
    let missing = required.iter().filter(|feature| !enabled.contains(**feature)).copied().collect::<Vec<_>>();
    if !missing.is_empty() {
        panic!("ARROWCOL_OAT_CLIENT_BUILD_TARGET_CPU={target_cpu} requires target features missing from CARGO_CFG_TARGET_FEATURE: {}",
            missing.join(", "));
    }
}

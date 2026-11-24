pub(crate) fn cargo_lto_setting(lto: &str) -> Option<&'static str> {
    match lto {
        "off" => Some("off"),
        "thin-local" => Some("false"),
        "thin" => Some("thin"),
        "fat" => Some("fat"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::cargo_lto_setting;

    #[test]
    fn lto_modes_map_to_distinct_cargo_settings() {
        let settings = ["off", "thin-local", "thin", "fat"]
            .map(|mode| cargo_lto_setting(mode).expect("supported client LTO mode"));
        assert_eq!(settings, ["off", "false", "thin", "fat"]);
    }
}

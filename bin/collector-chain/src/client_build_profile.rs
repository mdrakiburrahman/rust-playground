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
        let settings = ["off", "thin-local", "thin", "fat"].map(cargo_lto_setting);
        assert_eq!(settings, [Some("off"), Some("false"), Some("thin"), Some("fat")]);
    }
}

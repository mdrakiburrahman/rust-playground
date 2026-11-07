/// Redacts credentials, URIs, absolute paths and private identifiers from diagnostics.
///
/// Preserves causal text, replaces sensitive values, removes control characters,
/// and bounds the result to 1,024 UTF-8 bytes before truncation.
#[must_use]
pub fn safe_error_message(message: &str) -> String {
    use std::borrow::Cow;
    use std::sync::LazyLock;

    use regex::Regex;

    // Match loadgen kusto::redact_text (34b899) URI/path/GUID/UPN/JWT canaries
    // and auth-automation's named-secret/Bearer replacement convention.
    static REDACTIONS: LazyLock<Result<Vec<(Regex, &'static str)>, regex::Error>> = LazyLock::new(|| {
        [
            (
                r#"(?i)\b(?:authorization|proxy-authorization)\s*[:=]\s*(?:"[^"]*"|'[^']*'|(?:bearer|basic)\s+[^\s,;]+|[^\s,;]+)"#,
                "[REDACTED]",
            ),
            (r"(?i)\bbearer\s+[a-z0-9._~+/\-=]+", "[REDACTED]"),
            (
                r#"(?i)["']?\b(?:password|pwd|account_?key|key|accountname|sharedaccesssignature|sharedaccesskey|access[_-]?token|refresh[_-]?token|client[_-]?secret|sig|se|sv|sp)["']?\s*[:=]\s*(?:"[^"]*"|'[^']*'|\{(?:[^}]|}})*\}|[^\s;,&"'<>]+)"#,
                "[REDACTED]",
            ),
            (
                r#"(?i)(?:"[a-z][a-z0-9+.-]*://[^"]*"|'[a-z][a-z0-9+.-]*://[^']*')"#,
                "[REDACTED]",
            ),
            (
                r#"(?i)\b[a-z][a-z0-9+.-]*://[^\s"'<>)\]}]+"#,
                "[REDACTED]",
            ),
            (
                r"\b[a-zA-Z0-9_-]{8,}\.[a-zA-Z0-9_-]{8,}\.[a-zA-Z0-9_-]{8,}\b",
                "[REDACTED]",
            ),
            (
                r"(?i)\beyj[a-z0-9._-]{29,}",
                "[REDACTED]",
            ),
            (
                r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
                "[REDACTED]",
            ),
            (
                r#"[^\s"'`()\[\]{},;<>:@]+@[^\s"'`()\[\]{},;<>:@]+\.[^\s"'`()\[\]{},;<>:@]+"#,
                "[REDACTED]",
            ),
            (r#"["'](?:/|[a-zA-Z]:[\\/]|\\\\)[^"']*["']"#, "[REDACTED]"),
            (r#"\b[a-zA-Z]:[\\/][^\s"'<>),;\]}]+"#, "[REDACTED]"),
            (r#"\\\\[^\s"'<>),;\]}]+"#, "[REDACTED]"),
            (r#"(^|[\s"'(=,:])[/\\][^\s"'<>),;:\]}]+"#, " [REDACTED]"),
        ]
        .into_iter()
        .map(|(pattern, replacement)| Regex::new(pattern).map(|regex| (regex, replacement)))
        .collect()
    });
    let Ok(redactions) = REDACTIONS.as_ref() else {
        return "publication failed (diagnostic redaction unavailable)".to_owned();
    };
    let mut redacted = Cow::Borrowed(message);
    for (pattern, replacement) in redactions {
        redacted = Cow::Owned(pattern.replace_all(&redacted, *replacement).into_owned());
    }
    let mut safe = String::with_capacity(redacted.len().min(1_024));
    for character in redacted.chars() {
        if safe.len() >= 1_024 {
            break;
        }
        let character = if character.is_control() { ' ' } else { character };
        if safe.len() + character.len_utf8() > 1_024 {
            break;
        }
        safe.push(character);
    }
    if safe.is_empty() {
        "publication failed".to_owned()
    } else {
        safe
    }
}

#[cfg(test)]
mod tests {
    use super::safe_error_message;

    #[test]
    fn diagnostics_redact_private_values_and_remain_bounded_utf8() {
        let message = concat!(
            "Partial query failure: abfss://private@acct.dfs.core.windows.net/x?sig=CANARYSIG ",
            "on /subscriptions/11111111-1111-1111-1111-111111111111/resourceGroups/rg ",
            "bearer opaque-token /delta/private/x C:\\privateCanaryWindows\\x; ",
            "privateCanaryUser@example.invalid: schema Int16 vs UInt8"
        );
        let safe = safe_error_message(message);
        for canary in ["CANARYSIG", "acct.dfs", "11111111-1111", "opaque-token", "privateCanary"] {
            assert!(!safe.contains(canary), "{canary} leaked: {safe}");
        }
        assert!(safe.contains("Partial query failure"));
        assert!(safe.contains("schema Int16 vs UInt8"));
        let bounded = safe_error_message(&format!("{}\n", "界".repeat(1_100)));
        assert!(bounded.chars().count() <= 1_024);
        assert!(bounded.len() <= 1_024);
        assert!(!bounded.chars().any(char::is_control));
    }
}
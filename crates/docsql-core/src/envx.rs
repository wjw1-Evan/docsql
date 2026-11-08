//! Strict environment-variable spelling rules shared by the server, the
//! web console and the CLI configs. One parse per contract: a drifted
//! boolean rule once read `DOCSQL_READ_ONLY=1␍` (trailing CR) as false —
//! fail-open on a security flag.

/// Strict boolean env value: anything but `1/0/true/false` (trimmed) is an
/// error, never a silent false.
pub fn env_bool(name: &str, value: Option<String>) -> Result<bool, String> {
    match value {
        Some(v) if !v.trim().is_empty() => match v.trim() {
            "1" | "true" => Ok(true),
            "0" | "false" => Ok(false),
            other => Err(format!(
                "{name}: invalid boolean value {other:?} (expected 1/0/true/false)"
            )),
        },
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_bool_accepts_only_the_documented_spellings() {
        assert_eq!(env_bool("X", Some("1".into())), Ok(true));
        assert_eq!(env_bool("X", Some(" true ".into())), Ok(true));
        assert_eq!(env_bool("X", Some("0".into())), Ok(false));
        assert_eq!(env_bool("X", None), Ok(false));
        assert_eq!(env_bool("X", Some(String::new())), Ok(false));
        // Trailing CR/space is trimmed first (a .env newline once made
        // `DOCSQL_READ_ONLY=1␍` read as false).
        assert_eq!(env_bool("X", Some("1\r".into())), Ok(true));
        assert!(env_bool("X", Some("yes".into())).is_err());
    }
}

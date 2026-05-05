use anyhow::{Result, anyhow};
use toml_edit::{DocumentMut, Item, Table, table};

const BUILTIN_OVERRIDE_PATHS: &[&[&str]] = &[
    &["protocol", "activation_point"],
    &["protocol", "version"],
    &["network", "name"],
    &["core", "validator_slots"],
];

pub(crate) struct AppliedChainspecOverrides {
    pub(crate) contents: String,
    pub(crate) overwritten_builtin_paths: Vec<String>,
}

#[derive(Debug)]
struct ChainspecOverride {
    raw_path: String,
    path: Vec<String>,
    value: Item,
}

pub(crate) fn apply(contents: &str, raw_overrides: &[String]) -> Result<AppliedChainspecOverrides> {
    if raw_overrides.is_empty() {
        return Ok(AppliedChainspecOverrides {
            contents: contents.to_string(),
            overwritten_builtin_paths: Vec::new(),
        });
    }

    let overrides = raw_overrides
        .iter()
        .map(|raw| parse_override(raw))
        .collect::<Result<Vec<_>>>()?;
    let mut document = contents
        .parse::<DocumentMut>()
        .map_err(|err| anyhow!("failed to parse chainspec TOML: {err}"))?;
    let mut overwritten_builtin_paths = Vec::new();

    for override_value in overrides {
        if is_builtin_override_path(&override_value.path)
            && !overwritten_builtin_paths.contains(&override_value.raw_path)
        {
            overwritten_builtin_paths.push(override_value.raw_path.clone());
        }
        apply_override(&mut document, override_value)?;
    }

    Ok(AppliedChainspecOverrides {
        contents: document.to_string(),
        overwritten_builtin_paths,
    })
}

fn parse_override(raw: &str) -> Result<ChainspecOverride> {
    let (raw_path, raw_value) = raw
        .split_once('=')
        .ok_or_else(|| anyhow!("chainspec override '{raw}' must use KEY=VALUE syntax"))?;
    let raw_path = raw_path.trim();
    let raw_value = raw_value.trim();
    if raw_value.is_empty() {
        return Err(anyhow!(
            "chainspec override '{raw}' has an empty TOML value"
        ));
    }

    let path = parse_path(raw_path)?;
    let value = parse_value(raw, raw_value)?;

    Ok(ChainspecOverride {
        raw_path: path.join("."),
        path,
        value,
    })
}

fn parse_path(raw_path: &str) -> Result<Vec<String>> {
    if raw_path.is_empty() {
        return Err(anyhow!("chainspec override path must not be empty"));
    }

    raw_path
        .split('.')
        .map(|segment| {
            if segment.is_empty() {
                return Err(anyhow!(
                    "chainspec override path '{raw_path}' contains an empty segment"
                ));
            }
            if !is_simple_key_segment(segment) {
                return Err(anyhow!(
                    "chainspec override path segment '{segment}' is not a simple TOML key"
                ));
            }
            Ok(segment.to_string())
        })
        .collect()
}

fn is_simple_key_segment(segment: &str) -> bool {
    segment
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn parse_value(raw: &str, raw_value: &str) -> Result<Item> {
    let synthetic = format!("value = {raw_value}");
    let document = synthetic
        .parse::<DocumentMut>()
        .map_err(|err| anyhow!("invalid TOML value in chainspec override '{raw}': {err}"))?;
    document
        .as_item()
        .get("value")
        .cloned()
        .ok_or_else(|| anyhow!("chainspec override '{raw}' did not produce a TOML value"))
}

fn is_builtin_override_path(path: &[String]) -> bool {
    BUILTIN_OVERRIDE_PATHS
        .iter()
        .any(|builtin| path.iter().map(String::as_str).eq(builtin.iter().copied()))
}

fn apply_override(document: &mut DocumentMut, override_value: ChainspecOverride) -> Result<()> {
    let path_display = override_value.raw_path.clone();
    apply_to_table(
        document.as_table_mut(),
        &override_value.path,
        override_value.value,
        &path_display,
    )
}

fn apply_to_table(
    table_value: &mut Table,
    path: &[String],
    value: Item,
    path_display: &str,
) -> Result<()> {
    let key = &path[0];
    if path.len() == 1 {
        table_value.insert(key, value);
        return Ok(());
    }

    if !table_value.contains_key(key) {
        table_value.insert(key, table());
    }

    let item = table_value
        .get_key_value_mut(key)
        .map(|(_, item)| item)
        .ok_or_else(|| anyhow!("failed to create chainspec override table '{key}'"))?;
    let item_type = item.type_name();
    let child = item.as_table_mut().ok_or_else(|| {
        anyhow!(
            "cannot apply chainspec override '{path_display}': '{key}' is a {item_type}, not a table"
        )
    })?;
    apply_to_table(child, &path[1..], value, path_display)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "\
[protocol]
activation_point = 1
version = '1.0.0'

[network]
name = 'casper-dev'

[core]
validator_slots = 4
";

    fn apply_one(raw: &str) -> DocumentMut {
        apply(BASE, &[raw.to_string()])
            .unwrap()
            .contents
            .parse::<DocumentMut>()
            .unwrap()
    }

    #[test]
    fn applies_string_value() {
        let document = apply_one("highway.era_duration='1hour'");

        assert_eq!(
            document["highway"]["era_duration"]
                .as_value()
                .and_then(toml_edit::Value::as_str),
            Some("1hour")
        );
    }

    #[test]
    fn applies_integer_value() {
        let document = apply_one("core.minimum_era_height=1");

        assert_eq!(
            document["core"]["minimum_era_height"]
                .as_value()
                .and_then(toml_edit::Value::as_integer),
            Some(1)
        );
    }

    #[test]
    fn applies_boolean_value() {
        let document = apply_one("core.allow_auction_bids=false");

        assert_eq!(
            document["core"]["allow_auction_bids"]
                .as_value()
                .and_then(toml_edit::Value::as_bool),
            Some(false)
        );
    }

    #[test]
    fn applies_array_with_spaces() {
        let document = apply_one("core.test_values=[1, 10]");
        let array = document["core"]["test_values"]
            .as_value()
            .and_then(toml_edit::Value::as_array)
            .unwrap();

        assert_eq!(array.get(0).and_then(toml_edit::Value::as_integer), Some(1));
        assert_eq!(
            array.get(1).and_then(toml_edit::Value::as_integer),
            Some(10)
        );
    }

    #[test]
    fn applies_array_without_spaces() {
        let document = apply_one("core.test_values=[1,10]");
        let array = document["core"]["test_values"]
            .as_value()
            .and_then(toml_edit::Value::as_array)
            .unwrap();

        assert_eq!(array.get(0).and_then(toml_edit::Value::as_integer), Some(1));
        assert_eq!(
            array.get(1).and_then(toml_edit::Value::as_integer),
            Some(10)
        );
    }

    #[test]
    fn applies_inline_table() {
        let document = apply_one("core.rewards_handling={ type = 'standard' }");
        let table = document["core"]["rewards_handling"]
            .as_value()
            .and_then(toml_edit::Value::as_inline_table)
            .unwrap();

        assert_eq!(
            table.get("type").and_then(toml_edit::Value::as_str),
            Some("standard")
        );
    }

    #[test]
    fn rejects_missing_equals() {
        let err = parse_override("core.minimum_era_height").unwrap_err();

        assert!(err.to_string().contains("KEY=VALUE"));
    }

    #[test]
    fn rejects_empty_path_segments() {
        let err = parse_override("core..minimum_era_height=1").unwrap_err();

        assert!(err.to_string().contains("empty segment"));
    }

    #[test]
    fn rejects_invalid_toml_values() {
        let err = parse_override("core.minimum_era_height=[1,").unwrap_err();

        assert!(err.to_string().contains("invalid TOML value"));
    }

    #[test]
    fn reports_overwritten_builtin_paths() {
        let applied = apply(BASE, &["core.validator_slots=99".to_string()]).unwrap();

        assert_eq!(
            applied.overwritten_builtin_paths,
            vec!["core.validator_slots"]
        );
    }
}

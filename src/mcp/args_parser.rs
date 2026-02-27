use super::hex_to_bytes;
use anyhow::{Context, Result, anyhow};
use casper_types::AsymmetricType;
use casper_types::bytesrepr::{
    FromBytes, OPTION_NONE_TAG, OPTION_SOME_TAG, RESULT_ERR_TAG, RESULT_OK_TAG, ToBytes,
};
use casper_types::{CLType, CLValue, Key, PublicKey, RuntimeArgs, U128, U256, U512, URef};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct SessionArgInput {
    name: String,
    #[serde(rename = "type")]
    arg_type: String,
    value: Value,
}

pub(super) fn parse_session_args_json(input: &str) -> Result<Option<RuntimeArgs>> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(None);
    }

    if let Ok(runtime_args) = serde_json::from_str::<RuntimeArgs>(input) {
        return Ok(Some(runtime_args));
    }

    let args = serde_json::from_str::<Vec<SessionArgInput>>(input)
        .map_err(|err| anyhow!("invalid session_args_json: {}", err))?;
    let mut runtime_args = RuntimeArgs::new();
    for arg in args {
        let cl_value = parse_session_cl_value(&arg.arg_type, arg.value)
            .with_context(|| format!("invalid session arg '{}'", arg.name))?;
        runtime_args.insert_cl_value(arg.name, cl_value);
    }
    Ok(Some(runtime_args))
}

fn parse_session_cl_value(arg_type: &str, value: Value) -> Result<CLValue> {
    let cl_type = parse_cl_type(arg_type)
        .with_context(|| format!("failed to parse CLType '{}'", arg_type))?;
    let input = session_arg_value_to_input(value, &cl_type, arg_type)?;
    let bytes = parse_cl_value_for_type(&cl_type, &input)
        .with_context(|| format!("failed to parse value '{}' as {}", input, arg_type))?;
    Ok(CLValue::from_components(cl_type, bytes))
}

fn session_arg_value_to_input(value: Value, cl_type: &CLType, arg_type: &str) -> Result<String> {
    match value {
        Value::String(text) => Ok(text),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(flag) => Ok(if flag { "true" } else { "false" }.to_string()),
        Value::Null if matches!(cl_type, CLType::Option(_)) => Ok("None".to_string()),
        Value::Null if matches!(cl_type, CLType::Unit) => Ok("()".to_string()),
        Value::Null => Err(anyhow!(
            "{} value cannot be null for this CLType; use explicit value or Option<T>",
            arg_type
        )),
        other => Err(anyhow!(
            "{} value must be a string (and may also be number/bool/null for scalar/Option/Unit types), got {}",
            arg_type,
            other
        )),
    }
}

const MAX_TYPE_NESTING: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
enum TypeTokenKind {
    Ident(String),
    Number(u32),
    LAngle,
    RAngle,
    LBracket,
    RBracket,
    LParen,
    RParen,
    Comma,
}

#[derive(Debug, Clone)]
struct TypeToken {
    kind: TypeTokenKind,
    pos: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GenericKind {
    Option,
    List,
    Result,
    Map,
    Tuple1,
    Tuple2,
    Tuple3,
    ByteArray,
}

impl GenericKind {
    fn name(self) -> &'static str {
        match self {
            GenericKind::Option => "Option",
            GenericKind::List => "List",
            GenericKind::Result => "Result",
            GenericKind::Map => "Map",
            GenericKind::Tuple1 => "Tuple1",
            GenericKind::Tuple2 => "Tuple2",
            GenericKind::Tuple3 => "Tuple3",
            GenericKind::ByteArray => "ByteArray",
        }
    }

    fn expected_args(self) -> usize {
        match self {
            GenericKind::Option => 1,
            GenericKind::List => 1,
            GenericKind::Result => 2,
            GenericKind::Map => 2,
            GenericKind::Tuple1 => 1,
            GenericKind::Tuple2 => 2,
            GenericKind::Tuple3 => 3,
            GenericKind::ByteArray => 1,
        }
    }
}

#[derive(Debug)]
enum FrameKind {
    Root,
    Generic(GenericKind),
    Tuple,
}

#[derive(Debug)]
struct TypeFrame {
    kind: FrameKind,
    args: Vec<CLType>,
    expecting_type: bool,
    saw_comma: bool,
}

impl TypeFrame {
    fn new(kind: FrameKind) -> Self {
        Self {
            kind,
            args: Vec::new(),
            expecting_type: true,
            saw_comma: false,
        }
    }

    fn allow_trailing_comma(&self) -> bool {
        matches!(self.kind, FrameKind::Tuple)
    }
}

#[derive(Debug, Clone, Copy)]
struct PendingGeneric {
    kind: GenericKind,
}

impl PendingGeneric {
    fn expected_delimiter(self) -> &'static str {
        match self.kind {
            GenericKind::ByteArray => "'[' or '<'",
            _ => "'<'",
        }
    }
}

fn parse_cl_type(value: &str) -> Result<CLType> {
    let normalized: String = value.chars().filter(|ch| !ch.is_whitespace()).collect();
    if normalized == "()" {
        return Ok(CLType::Unit);
    }

    let tokens = tokenize_cl_type(value)?;
    if tokens.is_empty() {
        return Err(anyhow!("type string is empty"));
    }

    let mut frames = vec![TypeFrame::new(FrameKind::Root)];
    let mut pending_ctor: Option<PendingGeneric> = None;
    let mut index = 0usize;

    while index < tokens.len() {
        let token = &tokens[index];
        if let Some(pending) = pending_ctor
            && !matches!(token.kind, TypeTokenKind::LAngle | TypeTokenKind::LBracket)
        {
            return Err(anyhow!(
                "expected {} after {}",
                pending.expected_delimiter(),
                pending.kind.name()
            ));
        }

        match &token.kind {
            TypeTokenKind::Ident(ident) => {
                if let Some(pending) = pending_ctor {
                    return Err(anyhow!(
                        "expected {} after {}",
                        pending.expected_delimiter(),
                        pending.kind.name()
                    ));
                }
                let normalized = normalize_cl_type_ident(ident);
                match normalized.as_str() {
                    "accounthash" => push_type(&mut frames, CLType::ByteArray(32))?,
                    "option" => {
                        ensure_expecting_type(&frames)?;
                        pending_ctor = Some(PendingGeneric {
                            kind: GenericKind::Option,
                        });
                    }
                    "list" => {
                        ensure_expecting_type(&frames)?;
                        pending_ctor = Some(PendingGeneric {
                            kind: GenericKind::List,
                        });
                    }
                    "result" => {
                        ensure_expecting_type(&frames)?;
                        pending_ctor = Some(PendingGeneric {
                            kind: GenericKind::Result,
                        });
                    }
                    "map" => {
                        ensure_expecting_type(&frames)?;
                        pending_ctor = Some(PendingGeneric {
                            kind: GenericKind::Map,
                        });
                    }
                    "tuple1" => {
                        ensure_expecting_type(&frames)?;
                        pending_ctor = Some(PendingGeneric {
                            kind: GenericKind::Tuple1,
                        });
                    }
                    "tuple2" => {
                        ensure_expecting_type(&frames)?;
                        pending_ctor = Some(PendingGeneric {
                            kind: GenericKind::Tuple2,
                        });
                    }
                    "tuple3" => {
                        ensure_expecting_type(&frames)?;
                        pending_ctor = Some(PendingGeneric {
                            kind: GenericKind::Tuple3,
                        });
                    }
                    "bytearray" => {
                        ensure_expecting_type(&frames)?;
                        pending_ctor = Some(PendingGeneric {
                            kind: GenericKind::ByteArray,
                        });
                    }
                    "bool" => push_type(&mut frames, CLType::Bool)?,
                    "i32" => push_type(&mut frames, CLType::I32)?,
                    "i64" => push_type(&mut frames, CLType::I64)?,
                    "u8" => push_type(&mut frames, CLType::U8)?,
                    "u32" => push_type(&mut frames, CLType::U32)?,
                    "u64" => push_type(&mut frames, CLType::U64)?,
                    "u128" => push_type(&mut frames, CLType::U128)?,
                    "u256" => push_type(&mut frames, CLType::U256)?,
                    "u512" => push_type(&mut frames, CLType::U512)?,
                    "unit" => push_type(&mut frames, CLType::Unit)?,
                    "string" => push_type(&mut frames, CLType::String)?,
                    "key" => push_type(&mut frames, CLType::Key)?,
                    "uref" => push_type(&mut frames, CLType::URef)?,
                    "publickey" => push_type(&mut frames, CLType::PublicKey)?,
                    "any" => push_type(&mut frames, CLType::Any)?,
                    _ => return Err(anyhow!("unknown CLType '{}'", ident)),
                }
            }
            TypeTokenKind::Number(_) => {
                return Err(anyhow!("unexpected number at position {}", token.pos));
            }
            TypeTokenKind::LAngle => {
                let pending = pending_ctor
                    .take()
                    .ok_or_else(|| anyhow!("unexpected delimiter '<'"))?;
                if pending.kind == GenericKind::ByteArray {
                    let len = parse_byte_array_len(&tokens, &mut index, TypeTokenKind::RAngle)?;
                    push_type(&mut frames, CLType::ByteArray(len))?;
                } else {
                    push_frame(&mut frames, FrameKind::Generic(pending.kind))?;
                }
            }
            TypeTokenKind::RAngle => {
                if pending_ctor.is_some() {
                    return Err(anyhow!("unexpected delimiter '>'"));
                }
                close_frame(&mut frames, TypeTokenKind::RAngle)?;
            }
            TypeTokenKind::LBracket => {
                let pending = pending_ctor
                    .take()
                    .ok_or_else(|| anyhow!("unexpected delimiter '['"))?;
                if pending.kind != GenericKind::ByteArray {
                    return Err(anyhow!("unexpected '[' after {}", pending.kind.name()));
                }
                let len = parse_byte_array_len(&tokens, &mut index, TypeTokenKind::RBracket)?;
                push_type(&mut frames, CLType::ByteArray(len))?;
            }
            TypeTokenKind::RBracket => {
                return Err(anyhow!("unexpected delimiter ']'"));
            }
            TypeTokenKind::LParen => {
                if let Some(pending) = pending_ctor {
                    return Err(anyhow!("unexpected '(' after {}", pending.kind.name()));
                }
                if matches!(
                    tokens.get(index + 1),
                    Some(TypeToken {
                        kind: TypeTokenKind::RParen,
                        ..
                    })
                ) {
                    ensure_expecting_type(&frames)?;
                    push_type(&mut frames, CLType::Unit)?;
                    index += 1;
                } else {
                    ensure_expecting_type(&frames)?;
                    push_frame(&mut frames, FrameKind::Tuple)?;
                }
            }
            TypeTokenKind::RParen => {
                if pending_ctor.is_some() {
                    return Err(anyhow!("unexpected delimiter ')'"));
                }
                close_frame(&mut frames, TypeTokenKind::RParen)?;
            }
            TypeTokenKind::Comma => {
                if pending_ctor.is_some() {
                    return Err(anyhow!("unexpected delimiter ','"));
                }
                let frame = frames
                    .last_mut()
                    .ok_or_else(|| anyhow!("internal parser error: missing frame"))?;
                if matches!(frame.kind, FrameKind::Root) {
                    return Err(anyhow!("unexpected delimiter ','"));
                }
                if frame.expecting_type {
                    return Err(anyhow!("missing type before ','"));
                }
                frame.expecting_type = true;
                frame.saw_comma = true;
            }
        }
        index += 1;
    }

    if let Some(pending) = pending_ctor {
        return Err(anyhow!(
            "expected {} after {}",
            pending.expected_delimiter(),
            pending.kind.name()
        ));
    }

    if frames.len() != 1 {
        return Err(anyhow!("missing closing delimiter"));
    }
    let mut root = frames
        .pop()
        .ok_or_else(|| anyhow!("internal parser error: missing root frame"))?;
    if root.expecting_type {
        return Err(anyhow!("type string is incomplete"));
    }
    if root.args.len() != 1 {
        return Err(anyhow!("expected a single type"));
    }
    Ok(root.args.remove(0))
}

fn tokenize_cl_type(value: &str) -> Result<Vec<TypeToken>> {
    let mut tokens = Vec::new();
    let mut chars = value.char_indices().peekable();
    while let Some((idx, ch)) = chars.peek().copied() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }

        let kind = match ch {
            '<' => {
                chars.next();
                TypeTokenKind::LAngle
            }
            '>' => {
                chars.next();
                TypeTokenKind::RAngle
            }
            '[' => {
                chars.next();
                TypeTokenKind::LBracket
            }
            ']' => {
                chars.next();
                TypeTokenKind::RBracket
            }
            '(' => {
                chars.next();
                TypeTokenKind::LParen
            }
            ')' => {
                chars.next();
                TypeTokenKind::RParen
            }
            ',' => {
                chars.next();
                TypeTokenKind::Comma
            }
            _ if ch.is_ascii_alphabetic() => {
                let mut ident = String::new();
                while let Some((_, next)) = chars.peek().copied() {
                    if next.is_ascii_alphanumeric() || next == '-' || next == '_' {
                        ident.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                TypeTokenKind::Ident(ident)
            }
            _ if ch.is_ascii_digit() => {
                let mut digits = String::new();
                while let Some((_, next)) = chars.peek().copied() {
                    if next.is_ascii_digit() || next == '_' {
                        digits.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let normalized: String = digits.chars().filter(|c| *c != '_').collect();
                if normalized.is_empty() {
                    return Err(anyhow!("invalid number at position {}", idx));
                }
                let number = normalized
                    .parse::<u32>()
                    .map_err(|_| anyhow!("invalid number at position {}", idx))?;
                TypeTokenKind::Number(number)
            }
            _ => return Err(anyhow!("unexpected character '{}' at position {}", ch, idx)),
        };

        tokens.push(TypeToken { kind, pos: idx });
    }
    Ok(tokens)
}

fn normalize_cl_type_ident(ident: &str) -> String {
    ident
        .chars()
        .filter(|ch| *ch != '-' && *ch != '_')
        .flat_map(|ch| ch.to_lowercase())
        .collect()
}

fn push_frame(frames: &mut Vec<TypeFrame>, kind: FrameKind) -> Result<()> {
    if frames.len() > MAX_TYPE_NESTING {
        return Err(anyhow!("type nesting exceeds {}", MAX_TYPE_NESTING));
    }
    frames.push(TypeFrame::new(kind));
    Ok(())
}

fn ensure_expecting_type(frames: &[TypeFrame]) -> Result<()> {
    let frame = frames
        .last()
        .ok_or_else(|| anyhow!("internal parser error: missing frame"))?;
    if !frame.expecting_type {
        return Err(anyhow!("expected ',' or closing delimiter"));
    }
    Ok(())
}

fn push_type(frames: &mut [TypeFrame], cl_type: CLType) -> Result<()> {
    let frame = frames
        .last_mut()
        .ok_or_else(|| anyhow!("internal parser error: missing frame"))?;
    if !frame.expecting_type {
        return Err(anyhow!("expected ',' or closing delimiter"));
    }
    frame.args.push(cl_type);
    frame.expecting_type = false;
    Ok(())
}

fn close_frame(frames: &mut Vec<TypeFrame>, token: TypeTokenKind) -> Result<()> {
    let frame = frames
        .pop()
        .ok_or_else(|| anyhow!("internal parser error: missing frame"))?;
    match (token, &frame.kind) {
        (TypeTokenKind::RAngle, FrameKind::Generic(_)) => {}
        (TypeTokenKind::RParen, FrameKind::Tuple) => {}
        (TypeTokenKind::RAngle, FrameKind::Tuple) => return Err(anyhow!("unexpected '>'")),
        (TypeTokenKind::RParen, FrameKind::Generic(_)) => return Err(anyhow!("unexpected ')'")),
        (TypeTokenKind::RAngle, FrameKind::Root) | (TypeTokenKind::RParen, FrameKind::Root) => {
            return Err(anyhow!("unexpected closing delimiter"));
        }
        _ => return Err(anyhow!("unexpected closing delimiter")),
    }

    if frame.expecting_type
        && !(frame.allow_trailing_comma() && frame.saw_comma && !frame.args.is_empty())
    {
        return Err(anyhow!("missing type before closing delimiter"));
    }

    let cl_type = match frame.kind {
        FrameKind::Root => return Err(anyhow!("internal parser error: unexpected root frame")),
        FrameKind::Tuple => build_tuple_type(frame)?,
        FrameKind::Generic(kind) => build_generic_type(kind, frame.args)?,
    };
    push_type(frames, cl_type)
}

fn build_generic_type(kind: GenericKind, args: Vec<CLType>) -> Result<CLType> {
    let expected = kind.expected_args();
    if args.len() != expected {
        return Err(anyhow!(
            "{} expects {} type argument(s), got {}",
            kind.name(),
            expected,
            args.len()
        ));
    }
    let mut iter = args.into_iter();
    let cl_type = match kind {
        GenericKind::Option => CLType::Option(Box::new(iter.next().expect("len verified"))),
        GenericKind::List => CLType::List(Box::new(iter.next().expect("len verified"))),
        GenericKind::Result => CLType::Result {
            ok: Box::new(iter.next().expect("len verified")),
            err: Box::new(iter.next().expect("len verified")),
        },
        GenericKind::Map => CLType::Map {
            key: Box::new(iter.next().expect("len verified")),
            value: Box::new(iter.next().expect("len verified")),
        },
        GenericKind::Tuple1 => CLType::Tuple1([Box::new(iter.next().expect("len verified"))]),
        GenericKind::Tuple2 => CLType::Tuple2([
            Box::new(iter.next().expect("len verified")),
            Box::new(iter.next().expect("len verified")),
        ]),
        GenericKind::Tuple3 => CLType::Tuple3([
            Box::new(iter.next().expect("len verified")),
            Box::new(iter.next().expect("len verified")),
            Box::new(iter.next().expect("len verified")),
        ]),
        GenericKind::ByteArray => return Err(anyhow!("ByteArray expects a numeric length")),
    };
    Ok(cl_type)
}

fn build_tuple_type(frame: TypeFrame) -> Result<CLType> {
    if frame.args.is_empty() {
        return Err(anyhow!("tuple types cannot be empty"));
    }
    if frame.args.len() == 1 && !frame.saw_comma {
        return Err(anyhow!("tuple types require a comma, use '(T,)'"));
    }
    let len = frame.args.len();
    let mut iter = frame.args.into_iter();
    let cl_type = match len {
        1 => CLType::Tuple1([Box::new(iter.next().expect("len verified"))]),
        2 => CLType::Tuple2([
            Box::new(iter.next().expect("len verified")),
            Box::new(iter.next().expect("len verified")),
        ]),
        3 => CLType::Tuple3([
            Box::new(iter.next().expect("len verified")),
            Box::new(iter.next().expect("len verified")),
            Box::new(iter.next().expect("len verified")),
        ]),
        _ => return Err(anyhow!("tuple types support only 1, 2, or 3 elements")),
    };
    Ok(cl_type)
}

fn parse_byte_array_len(
    tokens: &[TypeToken],
    index: &mut usize,
    closing: TypeTokenKind,
) -> Result<u32> {
    let number = match tokens.get(*index + 1) {
        Some(TypeToken {
            kind: TypeTokenKind::Number(value),
            ..
        }) => *value,
        Some(token) => {
            return Err(anyhow!(
                "byte array length missing at position {}",
                token.pos
            ));
        }
        None => return Err(anyhow!("byte array length is missing")),
    };
    match tokens.get(*index + 2) {
        Some(TypeToken { kind, .. }) if *kind == closing => {}
        Some(token) => {
            let expected = match closing {
                TypeTokenKind::RBracket => "]",
                TypeTokenKind::RAngle => ">",
                _ => "closing delimiter",
            };
            return Err(anyhow!(
                "byte array length must be followed by '{}' at position {}",
                expected,
                token.pos
            ));
        }
        None => return Err(anyhow!("byte array length is missing closing delimiter")),
    }
    *index += 2;
    Ok(number)
}

fn parse_cl_value_for_type(cl_type: &CLType, input: &str) -> Result<Vec<u8>> {
    let trimmed = input.trim();
    if let CLType::Option(inner) = cl_type {
        if trimmed.eq_ignore_ascii_case("none") {
            return Ok(vec![OPTION_NONE_TAG]);
        }
        let has_hex_prefix = trimmed.starts_with("0x") || trimmed.starts_with("0X");
        if !has_hex_prefix && !requires_hex_session_value(inner.as_ref()) {
            let inner_bytes = parse_basic_cl_value(inner.as_ref(), trimmed)?;
            let mut bytes = Vec::with_capacity(1 + inner_bytes.len());
            bytes.push(OPTION_SOME_TAG);
            bytes.extend_from_slice(&inner_bytes);
            return Ok(bytes);
        }
    }
    if matches!(cl_type, CLType::Any) {
        return parse_hex_input(input);
    }
    if requires_hex_session_value(cl_type) {
        let bytes = parse_hex_input(input)?;
        let mut cursor = ValueCursor::new(&bytes);
        validate_bytes_for_cl_type(cl_type, &mut cursor)?;
        if !cursor.is_eof() {
            return Err(anyhow!("value has trailing bytes"));
        }
        Ok(bytes)
    } else {
        parse_basic_cl_value(cl_type, input)
    }
}

fn requires_hex_session_value(cl_type: &CLType) -> bool {
    !matches!(
        cl_type,
        CLType::Bool
            | CLType::I32
            | CLType::I64
            | CLType::U8
            | CLType::U32
            | CLType::U64
            | CLType::U128
            | CLType::U256
            | CLType::U512
            | CLType::Unit
            | CLType::String
            | CLType::Key
            | CLType::URef
            | CLType::PublicKey
    )
}

fn to_bytes_label<T: ToBytes>(value: &T, label: &'static str) -> Result<Vec<u8>> {
    value
        .to_bytes()
        .map_err(|err| anyhow!("invalid {} bytes: {}", label, err))
}

fn parse_basic_cl_value(cl_type: &CLType, input: &str) -> Result<Vec<u8>> {
    match cl_type {
        CLType::Bool => {
            let value = parse_bool_value(input)?;
            to_bytes_label(&value, "bool")
        }
        CLType::I32 => {
            let value = parse_i32_value(input)?;
            to_bytes_label(&value, "i32")
        }
        CLType::I64 => {
            let value = parse_i64_value(input)?;
            to_bytes_label(&value, "i64")
        }
        CLType::U8 => {
            let value = parse_u8_value(input)?;
            to_bytes_label(&value, "u8")
        }
        CLType::U32 => {
            let value = parse_u32_value(input)?;
            to_bytes_label(&value, "u32")
        }
        CLType::U64 => {
            let value = parse_u64_value(input)?;
            to_bytes_label(&value, "u64")
        }
        CLType::U128 => {
            let value = parse_u128_value(input)?;
            to_bytes_label(&value, "u128")
        }
        CLType::U256 => {
            let value = parse_u256_value(input)?;
            to_bytes_label(&value, "u256")
        }
        CLType::U512 => {
            let value = parse_u512_value(input)?;
            to_bytes_label(&value, "u512")
        }
        CLType::Unit => {
            let trimmed = input.trim();
            if trimmed.is_empty() || trimmed == "()" || trimmed.eq_ignore_ascii_case("unit") {
                Ok(Vec::new())
            } else {
                Err(anyhow!("unit values must be empty, '()' or 'unit'"))
            }
        }
        CLType::String => to_bytes_label(&input.to_string(), "string"),
        CLType::Key => {
            let value = Key::from_formatted_str(input.trim())
                .map_err(|err| anyhow!("invalid Key value: {}", err))?;
            to_bytes_label(&value, "key")
        }
        CLType::URef => {
            let value = URef::from_formatted_str(input.trim())
                .map_err(|err| anyhow!("invalid URef value: {}", err))?;
            to_bytes_label(&value, "uref")
        }
        CLType::PublicKey => {
            let value = PublicKey::from_hex(normalize_hex_input(input).as_bytes())
                .map_err(|err| anyhow!("invalid PublicKey value: {}", err))?;
            to_bytes_label(&value, "public-key")
        }
        CLType::Any => Err(anyhow!("Any values must be supplied as hex bytes")),
        _ => Err(anyhow!(
            "type requires hex-encoded bytes; provide value as 0x... for {:?}",
            cl_type
        )),
    }
}

fn parse_bool_value(input: &str) -> Result<bool> {
    match input.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(anyhow!("bool values must be true/false/1/0")),
    }
}

fn parse_i32_value(input: &str) -> Result<i32> {
    normalize_decimal(input)?
        .parse::<i32>()
        .map_err(|_| anyhow!("i32 value is out of range or invalid"))
}

fn parse_i64_value(input: &str) -> Result<i64> {
    normalize_decimal(input)?
        .parse::<i64>()
        .map_err(|_| anyhow!("i64 value is out of range or invalid"))
}

fn parse_u8_value(input: &str) -> Result<u8> {
    normalize_unsigned_decimal(input)?
        .parse::<u8>()
        .map_err(|_| anyhow!("u8 value is out of range or invalid"))
}

fn parse_u32_value(input: &str) -> Result<u32> {
    normalize_unsigned_decimal(input)?
        .parse::<u32>()
        .map_err(|_| anyhow!("u32 value is out of range or invalid"))
}

fn parse_u64_value(input: &str) -> Result<u64> {
    normalize_unsigned_decimal(input)?
        .parse::<u64>()
        .map_err(|_| anyhow!("u64 value is out of range or invalid"))
}

fn parse_u128_value(input: &str) -> Result<U128> {
    U128::from_dec_str(&normalize_unsigned_decimal(input)?)
        .map_err(|_| anyhow!("u128 value is invalid"))
}

fn parse_u256_value(input: &str) -> Result<U256> {
    U256::from_dec_str(&normalize_unsigned_decimal(input)?)
        .map_err(|_| anyhow!("u256 value is invalid"))
}

fn parse_u512_value(input: &str) -> Result<U512> {
    U512::from_dec_str(&normalize_unsigned_decimal(input)?)
        .map_err(|_| anyhow!("u512 value is invalid"))
}

fn normalize_decimal(input: &str) -> Result<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("value is empty"));
    }

    let mut normalized = String::new();
    for (index, ch) in trimmed.chars().enumerate() {
        if ch == '_' {
            continue;
        }
        if (ch == '-' || ch == '+') && index == 0 {
            normalized.push(ch);
            continue;
        }
        if ch.is_ascii_digit() {
            normalized.push(ch);
        } else {
            return Err(anyhow!("invalid decimal digit '{}'", ch));
        }
    }

    if normalized == "-" || normalized == "+" {
        return Err(anyhow!("value is missing digits"));
    }
    Ok(normalized)
}

fn normalize_unsigned_decimal(input: &str) -> Result<String> {
    let normalized = normalize_decimal(input)?;
    if normalized.starts_with('-') {
        return Err(anyhow!("unsigned values cannot be negative"));
    }
    Ok(normalized.trim_start_matches('+').to_string())
}

fn parse_hex_input(input: &str) -> Result<Vec<u8>> {
    let normalized = normalize_hex_input(input);
    if normalized.is_empty() {
        return Ok(Vec::new());
    }
    if !normalized.len().is_multiple_of(2) {
        return Err(anyhow!("hex input has odd length"));
    }
    hex_to_bytes(&normalized)
}

fn normalize_hex_input(input: &str) -> String {
    let trimmed = input.trim();
    let trimmed = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    trimmed
        .chars()
        .filter(|ch| !ch.is_whitespace() && *ch != '_')
        .collect()
}

struct ValueCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> ValueCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn is_eof(&self) -> bool {
        self.pos == self.bytes.len()
    }

    fn remaining_slice(&self) -> &'a [u8] {
        &self.bytes[self.pos..]
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.bytes.len().saturating_sub(self.pos) < len {
            return Err(anyhow!("value is shorter than expected"));
        }
        let start = self.pos;
        let end = start + len;
        self.pos = end;
        Ok(&self.bytes[start..end])
    }

    fn take_u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn take_u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        let mut buf = [0u8; 4];
        buf.copy_from_slice(bytes);
        Ok(u32::from_le_bytes(buf))
    }
}

#[derive(Clone, Copy)]
enum ValueTask<'a> {
    Type(&'a CLType),
    List {
        element: &'a CLType,
        remaining: u32,
    },
    Map {
        key: &'a CLType,
        value: &'a CLType,
        remaining: u32,
        expecting_key: bool,
    },
}

fn validate_bytes_for_cl_type<'a>(cl_type: &'a CLType, cursor: &mut ValueCursor<'a>) -> Result<()> {
    let mut stack = vec![ValueTask::Type(cl_type)];
    while let Some(task) = stack.pop() {
        match task {
            ValueTask::Type(cl_type) => match cl_type {
                CLType::Bool => consume_from_bytes::<bool>(cursor, "bool")?,
                CLType::I32 => consume_from_bytes::<i32>(cursor, "i32")?,
                CLType::I64 => consume_from_bytes::<i64>(cursor, "i64")?,
                CLType::U8 => consume_from_bytes::<u8>(cursor, "u8")?,
                CLType::U32 => consume_from_bytes::<u32>(cursor, "u32")?,
                CLType::U64 => consume_from_bytes::<u64>(cursor, "u64")?,
                CLType::U128 => consume_from_bytes::<U128>(cursor, "u128")?,
                CLType::U256 => consume_from_bytes::<U256>(cursor, "u256")?,
                CLType::U512 => consume_from_bytes::<U512>(cursor, "u512")?,
                CLType::Unit => {}
                CLType::String => consume_from_bytes::<String>(cursor, "string")?,
                CLType::Key => consume_from_bytes::<Key>(cursor, "key")?,
                CLType::URef => consume_from_bytes::<URef>(cursor, "uref")?,
                CLType::PublicKey => consume_from_bytes::<PublicKey>(cursor, "public-key")?,
                CLType::Option(inner) => {
                    let tag = cursor.take_u8()?;
                    match tag {
                        OPTION_NONE_TAG => {}
                        OPTION_SOME_TAG => stack.push(ValueTask::Type(inner.as_ref())),
                        _ => return Err(anyhow!("option tag must be 0x00 or 0x01")),
                    }
                }
                CLType::List(inner) => {
                    let length = cursor.take_u32()?;
                    stack.push(ValueTask::List {
                        element: inner.as_ref(),
                        remaining: length,
                    });
                }
                CLType::ByteArray(length) => {
                    let len = usize::try_from(*length)
                        .map_err(|_| anyhow!("byte array length is too large"))?;
                    cursor.take(len)?;
                }
                CLType::Result { ok, err } => {
                    let tag = cursor.take_u8()?;
                    match tag {
                        RESULT_ERR_TAG => stack.push(ValueTask::Type(err.as_ref())),
                        RESULT_OK_TAG => stack.push(ValueTask::Type(ok.as_ref())),
                        _ => return Err(anyhow!("result tag must be 0x00 or 0x01")),
                    }
                }
                CLType::Map { key, value } => {
                    let length = cursor.take_u32()?;
                    stack.push(ValueTask::Map {
                        key: key.as_ref(),
                        value: value.as_ref(),
                        remaining: length,
                        expecting_key: true,
                    });
                }
                CLType::Tuple1([t1]) => stack.push(ValueTask::Type(t1.as_ref())),
                CLType::Tuple2([t1, t2]) => {
                    stack.push(ValueTask::Type(t2.as_ref()));
                    stack.push(ValueTask::Type(t1.as_ref()));
                }
                CLType::Tuple3([t1, t2, t3]) => {
                    stack.push(ValueTask::Type(t3.as_ref()));
                    stack.push(ValueTask::Type(t2.as_ref()));
                    stack.push(ValueTask::Type(t1.as_ref()));
                }
                CLType::Any => return Err(anyhow!("Any values are not supported in nested forms")),
            },
            ValueTask::List { element, remaining } => {
                if remaining == 0 {
                    continue;
                }
                stack.push(ValueTask::List {
                    element,
                    remaining: remaining - 1,
                });
                stack.push(ValueTask::Type(element));
            }
            ValueTask::Map {
                key,
                value,
                remaining,
                expecting_key,
            } => {
                if remaining == 0 {
                    continue;
                }
                if expecting_key {
                    stack.push(ValueTask::Map {
                        key,
                        value,
                        remaining,
                        expecting_key: false,
                    });
                    stack.push(ValueTask::Type(key));
                } else {
                    stack.push(ValueTask::Map {
                        key,
                        value,
                        remaining: remaining - 1,
                        expecting_key: true,
                    });
                    stack.push(ValueTask::Type(value));
                }
            }
        }
    }
    Ok(())
}

fn consume_from_bytes<T: FromBytes>(
    cursor: &mut ValueCursor<'_>,
    label: &'static str,
) -> Result<()> {
    let remaining = cursor.remaining_slice();
    let (_value, remainder) =
        T::from_bytes(remaining).map_err(|err| anyhow!("invalid {} bytes: {}", label, err))?;
    let consumed = remaining
        .len()
        .checked_sub(remainder.len())
        .ok_or_else(|| anyhow!("internal parser error: invalid remainder length"))?;
    cursor.pos = cursor
        .pos
        .checked_add(consumed)
        .ok_or_else(|| anyhow!("value length overflow"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_session_args_json;
    use casper_types::bytesrepr::ToBytes;
    use std::collections::BTreeMap;

    #[test]
    fn parse_session_args_json_common_types() {
        let input = r#"[{"name":"count","type":"U64","value":"7"},{"name":"enabled","type":"Bool","value":true}]"#;
        let args = parse_session_args_json(input).unwrap().unwrap();
        let count = args.get("count").unwrap().clone().into_t::<u64>().unwrap();
        let enabled = args
            .get("enabled")
            .unwrap()
            .clone()
            .into_t::<bool>()
            .unwrap();
        assert_eq!(count, 7);
        assert!(enabled);
    }

    #[test]
    fn parse_session_args_json_supports_nested_cltypes_with_hex() {
        let list_hex = {
            let bytes = vec![1u64, 2u64, 3u64].to_bytes().unwrap();
            format!(
                "0x{}",
                bytes
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<String>()
            )
        };
        let map_hex = {
            let mut value = BTreeMap::<String, u64>::new();
            value.insert("counter".to_string(), 9);
            let bytes = value.to_bytes().unwrap();
            format!(
                "0x{}",
                bytes
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<String>()
            )
        };
        let input = format!(
            r#"[{{"name":"items","type":"List<U64>","value":"{}"}},{{"name":"lookup","type":"Map<String,U64>","value":"{}"}}]"#,
            list_hex, map_hex
        );
        let args = parse_session_args_json(&input).unwrap().unwrap();
        let items = args
            .get("items")
            .unwrap()
            .clone()
            .into_t::<Vec<u64>>()
            .unwrap();
        let lookup = args
            .get("lookup")
            .unwrap()
            .clone()
            .into_t::<BTreeMap<String, u64>>()
            .unwrap();
        assert_eq!(items, vec![1, 2, 3]);
        assert_eq!(lookup.get("counter"), Some(&9));
    }

    #[test]
    fn parse_session_args_json_supports_option_none_and_aliases() {
        let input = r#"[{"name":"maybe_count","type":"Option<U64>","value":null},{"name":"account","type":"account_hash","value":"0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"}]"#;
        let args = parse_session_args_json(input).unwrap().unwrap();
        let maybe_count = args
            .get("maybe_count")
            .unwrap()
            .clone()
            .into_t::<Option<u64>>()
            .unwrap();
        let account = args
            .get("account")
            .unwrap()
            .clone()
            .into_t::<[u8; 32]>()
            .unwrap();
        assert_eq!(maybe_count, None);
        assert_eq!(account[0], 0x00);
        assert_eq!(account[31], 0x1f);
    }
}

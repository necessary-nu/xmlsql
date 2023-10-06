use std::str::FromStr;

// use base64::Engine;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum Inferred {
    Empty,
    Whitespace,
    String,
    Boolean(bool),
    Int(u64),
    Float(f64),
    Uuid(Uuid),
    DateTime(speedate::DateTime),
    Time(speedate::Time),
    Date(speedate::Date),
    Duration(speedate::Duration),
    // Base64(Vec<u8>),
    Json(serde_json::Value),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InferredType {
    Empty,
    Whitespace,
    String,
    Boolean,
    Int,
    Float,
    Uuid,
    DateTime,
    Time,
    Date,
    Duration,
    // Base64,
    Json,
}

impl FromStr for InferredType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "empty" => Ok(InferredType::Empty),
            "whitespace" => Ok(InferredType::Whitespace),
            "string" => Ok(InferredType::String),
            "boolean" => Ok(InferredType::Boolean),
            "int" => Ok(InferredType::Int),
            "float" => Ok(InferredType::Float),
            "uuid" => Ok(InferredType::Uuid),
            "datetime" => Ok(InferredType::DateTime),
            "time" => Ok(InferredType::Time),
            "date" => Ok(InferredType::Date),
            "duration" => Ok(InferredType::Duration),
            // "base64" => Ok(InferredType::Base64),
            "json" => Ok(InferredType::Json),
            _ => Err(()),
        }
    }
}

impl Inferred {
    pub fn as_type(&self) -> InferredType {
        match self {
            Inferred::Empty => InferredType::Empty,
            Inferred::Whitespace => InferredType::Whitespace,
            Inferred::String => InferredType::String,
            Inferred::Boolean(_) => InferredType::Boolean,
            Inferred::Int(_) => InferredType::Int,
            Inferred::Float(_) => InferredType::Float,
            Inferred::Uuid(_) => InferredType::Uuid,
            Inferred::DateTime(_) => InferredType::DateTime,
            Inferred::Time(_) => InferredType::Time,
            Inferred::Date(_) => InferredType::Date,
            Inferred::Duration(_) => InferredType::Duration,
            // Inferred::Base64(_) => InferredType::Base64,
            Inferred::Json(_) => InferredType::Json,
        }
    }
}

impl InferredType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Whitespace => "whitespace",
            Self::String => "string",
            Self::Boolean => "boolean",
            Self::Int => "int",
            Self::Float => "float",
            Self::Uuid => "uuid",
            Self::DateTime => "datetime",
            Self::Time => "time",
            Self::Date => "date",
            Self::Duration => "duration",
            // Self::Base64 => "base64",
            Self::Json => "json",
        }
    }
}

pub(crate) fn infer_type(input: &str) -> Inferred {
    if input.is_empty() {
        return Inferred::Empty;
    }

    if input.trim().is_empty() {
        return Inferred::Whitespace;
    }

    if input.len() == 4 {
        if input.to_ascii_lowercase() == "true" {
            return Inferred::Boolean(true);
        }
    }

    if input.len() == 5 {
        if input.to_ascii_lowercase() == "false" {
            return Inferred::Boolean(false);
        }
    }

    if let Ok(num) = input.parse::<u64>() {
        return Inferred::Int(num);
    }

    if let Ok(num) = input.parse::<f64>() {
        return Inferred::Float(num);
    }

    if let Ok(uuid) = Uuid::parse_str(input) {
        return Inferred::Uuid(uuid);
    }

    if let Ok(dt) = speedate::DateTime::parse_str(input) {
        return Inferred::DateTime(dt);
    }

    if let Ok(dt) = speedate::Time::parse_str(input) {
        return Inferred::Time(dt);
    }

    if let Ok(dt) = speedate::Date::parse_str(input) {
        return Inferred::Date(dt);
    }

    if let Ok(dt) = speedate::Duration::parse_str(input) {
        return Inferred::Duration(dt);
    }

    // if let Ok(base64) = base64::engine::general_purpose::STANDARD.decode(input) {
    //     return Inferred::Base64(base64);
    // }

    if let Ok(json) = serde_json::from_str(input) {
        return Inferred::Json(json);
    }

    Inferred::String
}

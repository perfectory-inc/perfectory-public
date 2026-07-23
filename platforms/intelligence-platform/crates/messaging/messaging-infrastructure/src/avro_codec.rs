use apache_avro::{from_avro_datum, to_avro_datum, types::Value as AvroValue, Schema};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EventCodecError {
    #[error("{message}")]
    InvalidWireFormat { message: String },
    #[error("{message}")]
    Avro { message: String },
}

impl EventCodecError {
    pub fn safe_message(&self) -> &'static str {
        match self {
            Self::InvalidWireFormat { .. } => "invalid wire format",
            Self::Avro { .. } => "avro codec error",
        }
    }
}

pub struct ConfluentAvroCodec;

impl ConfluentAvroCodec {
    pub fn encode(
        schema_id: i32,
        schema: &Schema,
        value: &AvroValue,
    ) -> Result<Vec<u8>, EventCodecError> {
        let avro = to_avro_datum(schema, value.clone()).map_err(|err| EventCodecError::Avro {
            message: err.to_string(),
        })?;
        let mut bytes = Vec::with_capacity(avro.len() + 5);
        bytes.push(0);
        bytes.extend_from_slice(&schema_id.to_be_bytes());
        bytes.extend_from_slice(&avro);

        Ok(bytes)
    }

    pub fn decode(schema: &Schema, bytes: &[u8]) -> Result<(i32, AvroValue), EventCodecError> {
        if bytes.len() < 5 {
            return Err(EventCodecError::InvalidWireFormat {
                message:
                    "confluent wire-format payload must include a magic byte, schema id, and datum"
                        .to_string(),
            });
        }

        if bytes[0] != 0 {
            return Err(EventCodecError::InvalidWireFormat {
                message: "confluent wire-format payload must start with magic byte 0".to_string(),
            });
        }

        let schema_id = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        let mut datum = &bytes[5..];
        let value =
            from_avro_datum(schema, &mut datum, None).map_err(|err| EventCodecError::Avro {
                message: err.to_string(),
            })?;

        if !datum.is_empty() {
            return Err(EventCodecError::InvalidWireFormat {
                message: "confluent wire-format payload must not contain trailing bytes"
                    .to_string(),
            });
        }

        Ok((schema_id, value))
    }
}

use anyhow::Result;
use redis::{RedisError, aio::Connection};
use std::{collections::HashSet, hash::Hash};

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Command {
    name: String,
    arity: i64,
    flags: Vec<String>,
    first_key: i64,
    last_key: i64,
    step_count: i64,
    categories: Vec<String>,
}

impl Command {
    fn from_redis_values(values: &[redis::Value]) -> Option<Self> {
        let name = match &values[0] {
            redis::Value::Data(bytes) => {
                String::from_utf8(bytes.clone()).ok()?
            }
            _ => return None,
        };

        let arity = match &values[1] {
            redis::Value::Int(arity) => *arity,
            _ => return None,
        };

        let flags = match &values[2] {
            redis::Value::Bulk(values) => values
                .iter()
                .filter_map(|v| match v {
                    redis::Value::Data(bytes) => {
                        String::from_utf8(bytes.clone()).ok()
                    }
                    _ => None,
                })
                .collect(),
            _ => return None,
        };

        let first_key = match &values[3] {
            redis::Value::Int(num) => *num,
            _ => return None,
        };

        let last_key = match &values[4] {
            redis::Value::Int(num) => *num,
            _ => return None,
        };

        let step_count = match &values[5] {
            redis::Value::Int(num) => *num,
            _ => return None,
        };

        let categories = match &values[6] {
            redis::Value::Bulk(values) => values
                .iter()
                .filter_map(|v| match v {
                    redis::Value::Status(status_string) => {
                        Some(status_string.clone())
                    }
                    _ => None,
                })
                .collect(),
            _ => {
                return None;
            }
        };

        Some(Self {
            name,
            arity,
            flags,
            first_key,
            last_key,
            step_count,
            categories,
        })
    }

    pub async fn load(con: &mut Connection) -> Result<HashSet<Self>> {
        let commands: Vec<Vec<redis::Value>> = redis::cmd("COMMAND")
            .query_async(con)
            .await
            .map_err(|err| {
                RedisError::from((
                    redis::ErrorKind::IoError,
                    "Failed to execute COMMAND command",
                    err.to_string(),
                ))
            })?;

        let mut command_set = HashSet::new();

        for command_values in &commands {
            if let Some(command) = Self::from_redis_values(command_values) {
                println!("Parsed Command: {command:?}");
                command_set.insert(command);
            } else {
                println!("Failed to parse command: {command_values:?}");
            }
        }

        Ok(command_set)
    }
}


//use lazy_static::lazy_static;
//use regex::Regex;
//
//lazy_static! {
//    static ref REGEX: Regex = Regex::new(r"my_pattern").unwrap();
//}

//fn map_redis_result(redis_result: redis::RedisResult<T>) -> Result<T> {
//    redis_result.map_err(|e| anyhow!("Redis error: {}", e))
//}
// 1671007429.923882 [0 127.0.0.1:41104] "set" "foo" "bar two"

lazy_static! {
    static ref MONITOR_RE: Regex = Regex::new(r"([0-9.]+) \[([0-9]+) ([0-9.]+) \"([a-zA-Z]+)\" .*").unwrap();
}

struct MonitorMessage {
    time: f64,
    db: u32,
    host: String,
    port: u16,
    command: String,
    args: Vec<String>,
}

impl FromRedisValue for MonitorMessage {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        if let redis::Value::Bulk(bulk) = v {
            Ok(Self {
                time: 0.0,
                db: 0,
                host: "".to_string(),
                port: 0,
                command: "foo".to_string(),
                args: vec![],
            })
        } else {
            let err = redis::RedisError::from((redis::ErrorKind::IoError, "boop"));
            Err(err)
        }
    }
}


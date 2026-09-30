use redis::Value;
use redis_monitor::commands::{Command, Lookup};

pub fn reply() -> Value {
    fn convert(value: serde_json::Value) -> Value {
        match value {
            serde_json::Value::Null => Value::Nil,
            serde_json::Value::Number(n) => Value::Int(n.as_i64().unwrap()),
            serde_json::Value::String(s) => Value::BulkString(s.into_bytes()),
            serde_json::Value::Array(a) => {
                Value::Array(a.into_iter().map(convert).collect())
            }
            serde_json::Value::Object(fields) => Value::Array(
                fields
                    .into_iter()
                    .flat_map(|(key, value)| {
                        [Value::BulkString(key.into_bytes()), convert(value)]
                    })
                    .collect(),
            ),
            serde_json::Value::Bool(_) => panic!("unexpected fixture value"),
        }
    }
    convert(
        serde_json::from_str(include_str!("../fixtures/command.json")).unwrap(),
    )
}

pub fn lookup() -> Lookup {
    Command::from_reply(&reply()).unwrap().into()
}

pub struct Case {
    pub command: &'static str,
    pub args: Vec<&'static [u8]>,
    pub keys: Vec<&'static [u8]>,
}

pub fn cases() -> Vec<Case> {
    let mut cases = vec![
        Case {
            command: "georadius",
            args: vec![b"a", b"0", b"0", b"10", b"km", b"STORE", b"dest"],
            keys: vec![b"a", b"dest"],
        },
        Case {
            command: "georadiusbymember",
            args: vec![
                b"a",
                b"member",
                b"10",
                b"km",
                b"COUNT",
                b"3",
                b"ANY",
                b"STOREDIST",
                b"dest",
            ],
            keys: vec![b"a", b"dest"],
        },
        Case {
            command: "GET",
            args: vec![b"key"],
            keys: vec![b"key"],
        },
        Case {
            command: "sEt",
            args: vec![b"\xff\0key", b"value", b"GET"],
            keys: vec![b"\xff\0key"],
        },
        Case {
            command: "mget",
            args: vec![b"a", b"b", b"a", b""],
            keys: vec![b"a", b"b", b"a", b""],
        },
        Case {
            command: "mset",
            args: vec![b"a", b"value", b"b", b"value"],
            keys: vec![b"a", b"b"],
        },
        Case {
            command: "blpop",
            args: vec![b"a", b"b", b"0"],
            keys: vec![b"a", b"b"],
        },
        Case {
            command: "eval",
            args: vec![b"return 1", b"2", b"a", b"b", b"arg"],
            keys: vec![b"a", b"b"],
        },
        Case {
            command: "eval",
            args: vec![b"return 1", b"0"],
            keys: vec![],
        },
        Case {
            command: "fcall",
            args: vec![b"func", b"0", b"arg"],
            keys: vec![],
        },
        Case {
            command: "zunion",
            args: vec![b"2", b"a", b"b", b"WEIGHTS", b"1", b"2"],
            keys: vec![b"a", b"b"],
        },
        Case {
            command: "zunionstore",
            args: vec![b"dest", b"2", b"a", b"b"],
            keys: vec![b"dest", b"a", b"b"],
        },
        Case {
            command: "xread",
            args: vec![b"COUNT", b"10", b"sTrEaMs", b"a", b"b", b"0", b"$"],
            keys: vec![b"a", b"b"],
        },
    ];
    cases.extend(extended_cases());
    cases
}

fn extended_cases() -> Vec<Case> {
    vec![
        Case {
            command: "xreadgroup",
            args: vec![
                b"GROUP",
                b"group",
                b"consumer",
                b"STREAMS",
                b"a",
                b"b",
                b">",
                b">",
            ],
            keys: vec![b"a", b"b"],
        },
        Case {
            command: "object",
            args: vec![b"EnCoDiNg", b"a"],
            keys: vec![b"a"],
        },
        Case {
            command: "XGROUP",
            args: vec![b"create", b"a", b"group", b"$"],
            keys: vec![b"a"],
        },
        Case {
            command: "memory",
            args: vec![b"usage", b"a", b"SAMPLES", b"0"],
            keys: vec![b"a"],
        },
        Case {
            command: "ssubscribe",
            args: vec![b"channel"],
            keys: vec![],
        },
        Case {
            command: "ping",
            args: vec![],
            keys: vec![],
        },
        Case {
            command: "sort",
            args: vec![
                b"a", b"BY", b"STORE", b"GET", b"STORE", b"STORE", b"first",
                b"LIMIT", b"0", b"10", b"STORE", b"last",
            ],
            keys: vec![b"a", b"last"],
        },
        Case {
            command: "sort_ro",
            args: vec![b"a", b"BY", b"weight_*", b"GET", b"object_*"],
            keys: vec![b"a"],
        },
        Case {
            command: "migrate",
            args: vec![b"host", b"6379", b"a", b"0", b"1000", b"AUTH", b"KEYS"],
            keys: vec![b"a"],
        },
        Case {
            command: "migrate",
            args: vec![
                b"host", b"6379", b"", b"0", b"1000", b"AUTH2", b"KEYS",
                b"KEYS", b"COPY", b"KEYS", b"a", b"KEYS", b"b",
            ],
            keys: vec![b"a", b"KEYS", b"b"],
        },
        Case {
            command: "georadius",
            args: vec![
                b"a",
                b"0",
                b"0",
                b"10",
                b"km",
                b"STORE",
                b"STOREDIST",
                b"ASC",
            ],
            keys: vec![b"a", b"STOREDIST"],
        },
        Case {
            command: "georadiusbymember",
            args: vec![b"a", b"member", b"10", b"km", b"STOREDIST", b"STORE"],
            keys: vec![b"a", b"STORE"],
        },
    ]
}

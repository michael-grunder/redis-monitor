mod common;

use redis::Value;
use redis_monitor::commands::{Command, KeyError, Lookup};
use std::borrow::Cow;

#[test]
fn fixture_keys_are_borrowed_and_complete() {
    let lookup = common::lookup();
    for case in common::cases() {
        let keys: Vec<_> = lookup
            .keys(case.command.as_bytes(), &case.args)
            .unwrap_or_else(|e| panic!("{} {:?}: {e}", case.command, case.args))
            .collect();
        assert_eq!(keys, case.keys, "{} {:?}", case.command, case.args);
        for key in keys {
            assert!(case.args.iter().any(|arg| arg.as_ptr() == key.as_ptr()));
        }
    }
    let args = [
        Cow::Borrowed(b"binary\xff".as_slice()),
        Cow::Owned(b"value".to_vec()),
    ];
    assert_eq!(
        lookup.keys(b"set", &args).unwrap().collect::<Vec<_>>(),
        vec![b"binary\xff"]
    );
}

#[test]
fn malformed_arguments_never_produce_partial_keys() {
    let lookup = common::lookup();
    let cases: &[(&[u8], &[&[u8]])] = &[
        (b"get", &[]),
        (b"get", &[b"a", b"b"]),
        (b"eval", &[b"script", b"-1"]),
        (b"eval", &[b"script", b"+1", b"a"]),
        (b"eval", &[b"script", b"01", b"a"]),
        (b"eval", &[b"script", b" 1", b"a"]),
        (b"eval", &[b"script", b"", b"a"]),
        (b"eval", &[b"script", b"\xff", b"a"]),
        (b"eval", &[b"script", b"18446744073709551616"]),
        (b"eval", &[b"script", b"9223372036854775808"]),
        (b"eval", &[b"script", b"2", b"a"]),
        (b"zunionstore", &[b"dest", b"3", b"a"]),
        (b"xread", &[b"STREAMS", b"a", b"b", b"0"]),
        (b"xread", &[b"COUNT", b"1", b"STREAMS"]),
        (b"sort", &[b"a", b"STORE"]),
        (b"sort", &[b"a", b"LIMIT", b"0"]),
        (
            b"migrate",
            &[b"host", b"6379", b"a", b"0", b"10", b"KEYS", b"b"],
        ),
        (b"migrate", &[b"host", b"6379", b"", b"0", b"10", b"KEYS"]),
        (
            b"migrate",
            &[b"host", b"6379", b"a", b"0", b"10", b"AUTH2", b"user"],
        ),
    ];
    for (command, args) in cases {
        assert_eq!(
            lookup.keys(command, args).unwrap_err(),
            KeyError::InvalidArguments,
            "{command:?} {args:?}"
        );
    }
    assert_eq!(
        lookup.keys(b"not_a_command", &[] as &[&[u8]]).unwrap_err(),
        KeyError::UnknownCommand
    );
    assert_eq!(
        lookup
            .keys(b"object", &[b"unknown".as_slice(), b"a"])
            .unwrap_err(),
        KeyError::UnknownCommand
    );
    assert_eq!(
        lookup.keys(b"object", &[] as &[&[u8]]).unwrap_err(),
        KeyError::InvalidArguments
    );
}

#[test]
fn resp3_maps_and_sets_match_resp2_arrays() {
    fn maps(value: Value) -> Value {
        match value {
            Value::Array(values) => {
                let is_map = matches!(values.first(), Some(Value::BulkString(s)) if [b"flags".as_slice(), b"begin_search", b"type", b"spec", b"index", b"keyword", b"startfrom", b"lastkey", b"keystep", b"limit", b"keynumidx", b"firstkey"].contains(&s.as_slice()));
                let mut values = values.into_iter().map(maps);
                if is_map {
                    let mut pairs = Vec::new();
                    while let Some(key) = values.next() {
                        pairs.push((key, values.next().unwrap()));
                    }
                    Value::Map(pairs)
                } else {
                    let values: Vec<_> = values.collect();
                    if values.iter().all(|v| {
                        matches!(v, Value::BulkString(_) | Value::Map(_))
                    }) {
                        Value::Set(values)
                    } else {
                        Value::Array(values)
                    }
                }
            }
            other => other,
        }
    }
    let lookup: Lookup =
        Command::from_reply(&maps(common::reply())).unwrap().into();
    for case in common::cases() {
        assert_eq!(
            lookup
                .keys(case.command.as_bytes(), &case.args)
                .unwrap()
                .collect::<Vec<_>>(),
            case.keys
        );
    }
}

#[test]
fn legacy_metadata_and_malformed_replies() {
    let Value::Array(mut rows) = common::reply() else {
        unreachable!()
    };
    for row in &mut rows {
        let Value::Array(fields) = row else {
            unreachable!()
        };
        fields.truncate(6); // Redis before ACL categories and key specs.
    }
    let lookup: Lookup =
        Command::from_reply(&Value::Array(rows)).unwrap().into();
    assert_eq!(
        lookup
            .keys(b"mset", &[b"a".as_slice(), b"v", b"b", b"v"])
            .unwrap()
            .collect::<Vec<_>>(),
        vec![b"a", b"b"]
    );
    assert_eq!(
        lookup
            .keys(b"eval", &[b"script".as_slice(), b"0"])
            .unwrap_err(),
        KeyError::UnsupportedSpec
    );
    assert_eq!(
        lookup
            .keys(b"ssubscribe", &[b"channel".as_slice()])
            .unwrap()
            .count(),
        0
    );
    for value in [
        Value::Nil,
        Value::Array(vec![Value::Nil]),
        Value::Array(vec![Value::Array(vec![])]),
    ] {
        assert!(Command::from_reply(&value).is_err());
    }
}

#[tokio::test]
#[ignore = "requires KEY_TEST_REDIS_URL pointing to a Redis 7+/Valkey test server"]
async fn compare_live_command_getkeys() {
    let url =
        std::env::var("KEY_TEST_REDIS_URL").expect("set KEY_TEST_REDIS_URL");
    for protocol in ["", "?protocol=resp3"] {
        let client = redis::Client::open(format!("{url}{protocol}")).unwrap();
        let mut con = client.get_connection_manager().await.unwrap();
        let lookup: Lookup = Command::load(&mut con).await.unwrap().into();
        for case in common::cases() {
            let actual: Vec<_> = lookup
                .keys(case.command.as_bytes(), &case.args)
                .unwrap()
                .collect();
            // COMMAND GETKEYS returns an error for commands declaring no keys,
            // and includes NOT_KEY shard channels, which this API excludes.
            if case.command == "ping" || case.command == "ssubscribe" {
                continue;
            }
            // Valkey 8.1's GETKEYS keyword specs misclassify destination
            // names STORE/STOREDIST as more options (even returning ASC as
            // a key). Keep those grammar regression cases in the fixture;
            // use unambiguous geo calls for the server oracle below.
            if case.command.starts_with("georadius")
                && case
                    .keys
                    .iter()
                    .skip(1)
                    .any(|key| *key == b"STORE" || *key == b"STOREDIST")
            {
                assert_eq!(actual, case.keys);
                continue;
            }
            let expected: Vec<Vec<u8>> = redis::cmd("COMMAND")
                .arg("GETKEYS")
                .arg(case.command)
                .arg(&case.args)
                .query_async(&mut con)
                .await
                .unwrap();
            assert_eq!(actual, expected, "{} {:?}", case.command, case.args);
        }
    }
}

#[test]
fn malformed_specs_are_not_silently_discarded() {
    let Value::Array(rows) = common::reply() else {
        unreachable!()
    };
    let Value::Array(fields) = &rows[0] else {
        unreachable!()
    };
    for broken in [
        Value::Nil,
        Value::Array(vec![Value::Nil]),
        Value::Array(vec![Value::Array(vec![])]),
    ] {
        let mut row = fields.clone();
        row[8] = broken;
        let lookup: Lookup =
            Command::from_reply(&Value::Array(vec![Value::Array(row)]))
                .unwrap()
                .into();
        assert_eq!(
            lookup.keys(b"get", &[b"a".as_slice()]).unwrap_err(),
            KeyError::UnsupportedSpec
        );
    }
    let mut row = fields.clone();
    let Value::Array(specs) = &mut row[8] else {
        unreachable!()
    };
    specs.push(Value::Nil); // A known key followed by an unknown spec is not complete.
    let lookup: Lookup =
        Command::from_reply(&Value::Array(vec![Value::Array(row)]))
            .unwrap()
            .into();
    assert_eq!(
        lookup.keys(b"get", &[b"a".as_slice()]).unwrap_err(),
        KeyError::UnsupportedSpec
    );
}

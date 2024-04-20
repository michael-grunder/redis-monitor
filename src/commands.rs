use anyhow::{Context, Result};
use redis::{aio::Connection, Value};
use serde::Deserialize;
use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
};

#[derive(Debug, Deserialize)]
pub struct KeyInfo {
    first: u64,
    last: u64,
    step: u64,
}

#[derive(Debug, Deserialize)]
pub struct Command {
    name: String,
    arity: u64,
    keys: KeyInfo,
}

impl PartialEq for Command {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Hash for Command {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl Command {
    //fn get_name_and_arity(values: &Vec<Vec<redis::Value>>) -> (String, u64) {
    //    let cmd = match &values[0] {
    //        redis::Value::Data(bytes) => std::str::from_utf8(bytes).unwrap().to_string(),
    //        _ => panic!("Command is not a string"),
    //    };

    //    let arity = match &values[1] {
    //        redis::Value::Int(n) => *n as u64,
    //        _ => panic!("Arity is not a number?"),
    //    };

    //    (cmd, arity)
    //}

    //pub fn from_value(v: Vec<Vec<redis::Value>>) -> Self {
    //    let (name, arity) = Self::get_name_and_arity(&v);

    //    Self {
    //        name,
    //        arity,
    //        keys: KeyInfo {
    //            first: 0,
    //            last: 0,
    //            step: 0,
    //        },
    //    }
    //}
    pub async fn load(con: &mut Connection) -> Result<HashSet<Command>> {
        let commands: Vec<Vec<redis::Value>> = redis::cmd("COMMAND")
            .query_async(con)
            .await
            .context("Failed to execute COMMAND command")?;

        for command in commands.iter() {
            if let redis::Value::Data(s) = &command[0] {
                println!("Command name: {s:?}");
            }
            println!("{command:#?}");
        }

        Ok(HashSet::new())
    }
}

//[
//    string-data('"decrby"'),
//    int(3),
//    bulk(status("write"), status("denyoom"), status("fast")),
//    int(1),
//    int(1),
//    int(1),
//    bulk(status("@write"), status("@string"), status("@fast")),
//    bulk(),
//    bulk(bulk(string-data('"flags"'), bulk(status("RW"), status("access"), status("update")), string-data('"begin_search"'), bulk(string-data('"type"'), string-data('"index"'), string-data('"spec"'), bulk(string-data('"index"'), int(1))), string-data('"find_keys"'), bulk(string-data('"type"'), string-data('"range"'), string-data('"spec"'), bulk(string-data('"lastkey"'), int(0), string-data('"keystep"'), int(1), string-data('"limit"'), int(0))))),
//    bulk(),
//]

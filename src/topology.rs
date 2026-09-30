//! Cold-path cluster discovery and connection reconciliation. Record processing
//! stays in the existing source tasks; unchanged nodes retain those tasks.
use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tokio::{sync::watch, task::JoinSet};

use crate::{
    config::ServerAuth,
    connection::{Cluster, Monitor, ServerAddr, TlsConfig},
    pipeline::{self, Pipeline},
};

pub struct ClusterGroup {
    pub name: Option<String>,
    pub seeds: Vec<ServerAddr>,
    pub auth: ServerAuth,
    pub tls: Option<Arc<TlsConfig>>,
    pub replicas: bool,
    pub cluster: Cluster,
}

struct Target {
    id: String,
    monitor: Monitor,
}

impl ClusterGroup {
    fn targets(&self) -> HashMap<ServerAddr, Target> {
        let mut targets = HashMap::new();
        for primary in self.cluster.get_nodes() {
            let nodes = std::iter::once(&primary)
                .chain(primary.replicas.iter().filter(|_| self.replicas));
            for node in nodes {
                targets.insert(
                    node.addr.clone(),
                    Target {
                        id: node.id.clone(),
                        monitor: Monitor::new(
                            Some(self.name.as_deref().unwrap_or(&node.id)),
                            node.addr.clone(),
                            self.tls.clone(),
                            self.auth.clone(),
                        ),
                    },
                );
            }
        }
        targets
    }

    pub fn monitors(&self) -> Vec<Monitor> {
        self.targets().into_values().map(|t| t.monitor).collect()
    }

    /// CLI arguments may name multiple seeds of the same cluster, or distinct
    /// clusters. Merge overlapping discoveries so one node gets one monitor.
    pub fn overlaps(&self, other: &Self) -> bool {
        let ids = |cluster: &Cluster| {
            cluster
                .get_nodes()
                .into_iter()
                .flat_map(|node| {
                    std::iter::once(node.id)
                        .chain(node.replicas.into_iter().map(|r| r.id))
                })
                .collect::<std::collections::HashSet<_>>()
        };
        !ids(&self.cluster).is_disjoint(&ids(&other.cluster))
    }

    async fn refresh(&self) -> Result<Cluster> {
        // Prefer the last known members, so losing an original seed does not
        // prevent future refreshes. Try replicas too, even in primary-only mode.
        let mut candidates = Vec::new();
        for primary in self.cluster.get_nodes() {
            candidates.push(primary.addr);
            candidates.extend(primary.replicas.into_iter().map(|n| n.addr));
        }
        candidates.extend(self.seeds.iter().cloned());
        let mut seen = std::collections::HashSet::new();
        candidates.retain(|address| seen.insert(address.clone()));
        Cluster::from_seeds(&candidates, &self.auth, self.tls.as_deref()).await
    }
}

struct Active {
    id: String,
    stop: watch::Sender<bool>,
    task: tokio::task::Id,
}

#[derive(Default)]
struct Monitors {
    active: HashMap<ServerAddr, Active>,
    tasks: JoinSet<()>,
}

impl Monitors {
    fn reconcile(
        &mut self,
        desired: &HashMap<ServerAddr, Target>,
        pipeline: &Pipeline,
    ) {
        for (address, active) in &self.active {
            if desired.get(address).is_none_or(|t| t.id != active.id) {
                active.stop.send_replace(true);
            }
        }
        // Complete a retirement wave before starting additions. Otherwise a
        // slow sink plus repeated topology churn could accumulate generations
        // of blocked producers. Unchanged monitors keep running throughout.
        if self.active.values().any(|a| *a.stop.borrow()) {
            return;
        }
        for (address, target) in desired {
            // A retiring generation must drain before its address or node ID
            // can be reused. This also bounds tasks during repeated remaps.
            if self.active.contains_key(address)
                || self.active.values().any(|a| a.id == target.id)
            {
                continue;
            }
            let (stop, shutdown) = watch::channel(false);
            let task = self
                .tasks
                .spawn(pipeline::run_monitor(
                    target.monitor.clone(),
                    pipeline.clone(),
                    shutdown,
                ))
                .id();
            self.active.insert(
                address.clone(),
                Active {
                    id: target.id.clone(),
                    stop,
                    task,
                },
            );
        }
    }

    fn completed(
        &mut self,
        result: std::result::Result<
            (tokio::task::Id, ()),
            tokio::task::JoinError,
        >,
    ) -> Result<bool> {
        let task = match &result {
            Ok((id, ())) => *id,
            Err(error) => error.id(),
        };
        let retiring = self
            .active
            .values()
            .find(|active| active.task == task)
            .is_some_and(|active| *active.stop.borrow());
        self.active.retain(|_, active| active.task != task);
        result.context("Cluster monitor task failed")?;
        Ok(retiring)
    }

    async fn drain(&mut self) -> Result<()> {
        for active in self.active.values() {
            active.stop.send_replace(true);
        }
        let mut failure = None;
        while let Some(result) = self.tasks.join_next_with_id().await {
            if let Err(error) = self.completed(result) {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

/// One refresh at a time per cluster, with an interval after completion rather
/// than catch-up ticks. A failed refresh leaves the previous targets untouched.
pub async fn run(
    mut group: ClusterGroup,
    interval: Duration,
    pipeline: Pipeline,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut monitors = Monitors::default();
    let mut desired = group.targets();
    if *shutdown.borrow() {
        return Ok(());
    }
    monitors.reconcile(&desired, &pipeline);
    let mut failure = None;
    'running: loop {
        let result = {
            let refresh = async {
                tokio::time::sleep(interval).await;
                group.refresh().await
            };
            tokio::pin!(refresh);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.wait_for(|stop| *stop) => break 'running,
                    result = monitors.tasks.join_next_with_id(), if !monitors.tasks.is_empty() => {
                        if let Some(result) = result {
                            match monitors.completed(result) {
                                // An unstopped source only returns normally when
                                // output closes. Do not reconnect in a loop while
                                // the writer is broadcasting global shutdown.
                                Ok(false) => break 'running,
                                Ok(true) => monitors.reconcile(&desired, &pipeline),
                                Err(error) => {
                                    failure = Some(error);
                                    break 'running;
                                }
                            }
                        }
                    }
                    result = &mut refresh => break result,
                }
            }
        };
        match result {
            Ok(cluster) => {
                group.cluster = cluster;
                desired = group.targets();
                monitors.reconcile(&desired, &pipeline);
            }
            Err(error) => eprintln!(
                "Cluster {} refresh failed; retaining current monitors: {error:#}",
                group.name.as_deref().unwrap_or("(CLI)")
            ),
        }
    }
    let drained = monitors.drain().await;
    failure.map_or(drained, Err)
}

#[cfg(test)]
mod tests {
    use redis::Value;
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
        sync::{mpsc, oneshot},
        time::timeout,
    };

    use super::*;
    use crate::pipeline::{
        BatchConfig,
        tests::{pipeline, test_io},
    };

    fn slot(nodes: &[(&str, u16)]) -> Value {
        Value::Array(
            std::iter::once(Value::Int(0))
                .chain(std::iter::once(Value::Int(16383)))
                .chain(nodes.iter().map(|(id, port)| {
                    Value::Array(vec![
                        Value::BulkString(b"127.0.0.1".to_vec()),
                        Value::Int(i64::from(*port)),
                        Value::BulkString(id.as_bytes().to_vec()),
                    ])
                }))
                .collect(),
        )
    }

    fn cluster(nodes: &[(&str, u16)]) -> Cluster {
        Cluster::from_slots(Value::Array(vec![slot(nodes)])).unwrap()
    }

    fn group(nodes: &[(&str, u16)], replicas: bool) -> ClusterGroup {
        ClusterGroup {
            name: None,
            seeds: Vec::new(),
            auth: ServerAuth::default(),
            tls: None,
            replicas,
            cluster: cluster(nodes),
        }
    }

    async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
        timeout(Duration::from_secs(5), future)
            .await
            .expect("test stalled")
    }

    struct FakeNode {
        port: u16,
        queries: mpsc::Receiver<oneshot::Sender<Vec<u8>>>,
        monitors: mpsc::Receiver<TcpStream>,
        server: tokio::task::JoinHandle<()>,
    }

    impl FakeNode {
        async fn new() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let (queries_tx, queries) = mpsc::channel(16);
            let (monitors_tx, monitors) = mpsc::channel(16);
            let server = tokio::spawn(async move {
                let mut clients = JoinSet::new();
                loop {
                    tokio::select! {
                        accepted = listener.accept() => {
                            let (stream, _) = accepted.unwrap();
                            let queries = queries_tx.clone();
                            let monitors = monitors_tx.clone();
                            clients.spawn(async move {
                                let mut reader = BufReader::new(stream);
                                loop {
                                    let mut header = String::new();
                                    if reader.read_line(&mut header).await.unwrap() == 0 {
                                        return;
                                    }
                                    let count: usize = header.trim()[1..].parse().unwrap();
                                    let mut args = Vec::new();
                                    for _ in 0..count {
                                        header.clear();
                                        reader.read_line(&mut header).await.unwrap();
                                        let len: usize = header.trim()[1..].parse().unwrap();
                                        let mut arg = vec![0; len + 2];
                                        reader.read_exact(&mut arg).await.unwrap();
                                        arg.truncate(len);
                                        args.push(arg);
                                    }
                                    if args[0] == b"MONITOR" {
                                        reader.get_mut().write_all(b"+OK\r\n").await.unwrap();
                                        let _ = monitors.send(reader.into_inner()).await;
                                        return;
                                    }
                                    let reply = if args[0] == b"CLUSTER" {
                                        let (tx, rx) = oneshot::channel();
                                        if queries.send(tx).await.is_err() { return; }
                                        let Ok(reply) = rx.await else { return; };
                                        reply
                                    } else {
                                        b"+OK\r\n".to_vec()
                                    };
                                    if reader.get_mut().write_all(&reply).await.is_err() {
                                        return;
                                    }
                                }
                            });
                        }
                        result = clients.join_next(), if !clients.is_empty() => {
                            result.unwrap().unwrap();
                        }
                    }
                }
            });
            Self {
                port,
                queries,
                monitors,
                server,
            }
        }

        fn address(&self) -> ServerAddr {
            ServerAddr::from_tcp_addr("127.0.0.1", self.port)
        }

        async fn query(&mut self) -> oneshot::Sender<Vec<u8>> {
            within(self.queries.recv()).await.unwrap()
        }

        async fn monitor(&mut self) -> TcpStream {
            within(self.monitors.recv()).await.unwrap()
        }
    }

    impl Drop for FakeNode {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    fn reply(nodes: &[(&str, u16)]) -> Vec<u8> {
        let mut reply =
            format!("*1\r\n*{}\r\n:0\r\n:16383\r\n", nodes.len() + 2);
        for (id, port) in nodes {
            use std::fmt::Write;
            write!(
                reply,
                "*3\r\n$9\r\n127.0.0.1\r\n:{port}\r\n${}\r\n{id}\r\n",
                id.len()
            )
            .unwrap();
        }
        reply.into_bytes()
    }

    #[test]
    fn roles_names_and_duplicate_slot_ranges() {
        let mut group = group(&[("a", 1), ("b", 2)], false);
        assert_eq!(group.monitors().len(), 1);
        assert_eq!(group.monitors()[0].name.as_deref(), Some("a"));
        group.name = Some("cache".into());
        group.replicas = true;
        let monitors = group.monitors();
        assert_eq!(monitors.len(), 2);
        assert!(monitors.iter().all(|m| m.name.as_deref() == Some("cache")));
        group.cluster = Cluster::from_slots(Value::Array(vec![
            slot(&[("a", 1), ("b", 2)]),
            slot(&[("a", 1), ("c", 3)]),
        ]))
        .unwrap();
        assert_eq!(group.monitors().len(), 3);
        assert!(group.overlaps(&super::tests::group(&[("a", 1)], false)));
    }

    #[test]
    fn malformed_and_conflicting_topologies_are_rejected() {
        for value in [
            Value::Nil,
            Value::Array(vec![]),
            Value::Array(vec![Value::Int(1)]),
            Value::Array(vec![slot(&[])]),
            Value::Array(vec![slot(&[("a", 0)])]),
            Value::Array(vec![slot(&[("", 1)])]),
            Value::Array(vec![slot(&[("a", 1)]), slot(&[("a", 2)])]),
            Value::Array(vec![slot(&[("a", 1)]), slot(&[("b", 1)])]),
            Value::Array(vec![slot(&[("a", 1), ("b", 2)]), slot(&[("b", 2)])]),
        ] {
            assert!(Cluster::from_slots(value).is_err());
        }
        for (start, end) in [(-1, 10), (2, 1), (0, 16384)] {
            let Value::Array(mut fields) = slot(&[("a", 1)]) else {
                unreachable!();
            };
            fields[0] = Value::Int(start);
            fields[1] = Value::Int(end);
            assert!(
                Cluster::from_slots(Value::Array(vec![Value::Array(fields)]))
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn unchanged_and_promoted_nodes_keep_their_tasks() {
        let a = FakeNode::new().await;
        let b = FakeNode::new().await;
        let mut group = group(&[("a", a.port), ("b", b.port)], true);
        let (io, _rx) = test_io(16, 4096);
        let pipeline = pipeline(io, BatchConfig::new(false));
        let mut monitors = Monitors::default();
        monitors.reconcile(&group.targets(), &pipeline);
        let original: HashMap<_, _> = monitors
            .active
            .iter()
            .map(|(addr, a)| (addr.clone(), a.task))
            .collect();
        group.cluster = cluster(&[("b", b.port), ("a", a.port)]);
        monitors.reconcile(&group.targets(), &pipeline);
        for (address, active) in &monitors.active {
            assert_eq!(original[address], active.task);
            assert!(!*active.stop.borrow());
        }
        within(monitors.drain()).await.unwrap();
    }

    #[tokio::test]
    async fn address_changes_and_churn_wait_for_retiring_tasks() {
        let node = FakeNode::new().await;
        let old_address = node.address();
        let mut monitors = Monitors::default();
        let (stop, _shutdown) = watch::channel(false);
        let (release, blocked) = oneshot::channel();
        let task = monitors
            .tasks
            .spawn(async {
                blocked.await.unwrap();
            })
            .id();
        monitors.active.insert(
            old_address.clone(),
            Active {
                id: "a".into(),
                stop,
                task,
            },
        );
        let (io, _rx) = test_io(16, 4096);
        let pipeline = pipeline(io, BatchConfig::new(false));
        let moved = FakeNode::new().await;
        let mut group = group(&[("a", moved.port)], false);
        monitors.reconcile(&group.targets(), &pipeline);
        assert!(*monitors.active[&old_address].stop.borrow());
        assert_eq!(monitors.tasks.len(), 1);
        for id in ["b", "c", "d"] {
            group.cluster = cluster(&[(id, moved.port)]);
            monitors.reconcile(&group.targets(), &pipeline);
            assert_eq!(monitors.tasks.len(), 1);
        }
        release.send(()).unwrap();
        let completed =
            within(monitors.tasks.join_next_with_id()).await.unwrap();
        monitors.completed(completed).unwrap();
        monitors.reconcile(&group.targets(), &pipeline);
        assert_eq!(monitors.active[&moved.address()].id, "d");
        within(monitors.drain()).await.unwrap();
    }

    #[tokio::test]
    async fn refresh_uses_known_members_when_original_seed_is_gone() {
        let mut member = FakeNode::new().await;
        let mut group = group(&[("a", member.port)], false);
        group.seeds = vec![ServerAddr::from_tcp_addr("127.0.0.1", 0)];
        let port = member.port;
        let (cluster, ()) = tokio::join!(group.refresh(), async {
            member.query().await.send(reply(&[("a", port)])).unwrap();
        });
        assert_eq!(cluster.unwrap().get_nodes()[0].addr, member.address());
    }

    #[tokio::test]
    async fn discovery_falls_back_after_a_failed_candidate() {
        let mut failed = FakeNode::new().await;
        let mut healthy = FakeNode::new().await;
        let seeds = vec![failed.address(), healthy.address()];
        let auth = ServerAuth::default();
        let port = healthy.port;
        let (cluster, ()) =
            tokio::join!(Cluster::from_seeds(&seeds, &auth, None), async {
                failed
                    .query()
                    .await
                    .send(b"-ERR unavailable\r\n".to_vec())
                    .unwrap();
                healthy.query().await.send(reply(&[("b", port)])).unwrap();
            });
        assert_eq!(cluster.unwrap().get_nodes()[0].addr, healthy.address());
    }

    #[tokio::test]
    async fn supervisor_retains_failed_refresh_then_remaps_and_cancels_discovery()
     {
        let mut a = FakeNode::new().await;
        let mut b = FakeNode::new().await;
        let group = group(&[("a", a.port)], false);
        let (io, rx) = test_io(1, 4096);
        let pipeline = pipeline(io, BatchConfig::new(true));
        let (shutdown, stop) = watch::channel(false);
        let running =
            tokio::spawn(run(group, Duration::from_millis(1), pipeline, stop));
        let mut first = a.monitor().await;
        a.query()
            .await
            .send(b"-ERR refresh failed\r\n".to_vec())
            .unwrap();
        // The next query proves the failed refresh has been handled. The old
        // monitor must still emit records without another MONITOR connection.
        let next = a.query().await;
        first.write_all(b"+1.0 [0 lua] \"PING\"\r\n").await.unwrap();
        drop(within(rx.recv_async()).await.unwrap());
        assert!(a.monitors.try_recv().is_err());
        next.send(reply(&[("b", b.port)])).unwrap();
        assert_eq!(within(first.read(&mut [0])).await.unwrap(), 0);
        let mut second = b.monitor().await;
        // Leave this discovery query unanswered. Shutdown must cancel it and
        // close the monitor without waiting for the discovery timeout.
        let _stalled = b.query().await;
        shutdown.send_replace(true);
        within(running).await.unwrap().unwrap();
        assert_eq!(within(second.read(&mut [0])).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn cli_seeds_share_a_supervisor_but_distinct_clusters_do_not() {
        use clap::Parser;
        let mut a = FakeNode::new().await;
        let mut b = FakeNode::new().await;
        let a_addr = a.address().to_string();
        let b_addr = b.address().to_string();
        let options = crate::Options::try_parse_from([
            "redis-monitor",
            "--cluster",
            &a_addr,
            &a_addr,
            &b_addr,
        ])
        .unwrap();
        let a_port = a.port;
        let b_port = b.port;
        let auth = ServerAuth::default();
        let (sources, ()) = tokio::join!(
            crate::process_cluster_instances(&options, None, &auth),
            async {
                for _ in 0..2 {
                    a.query().await.send(reply(&[("a", a_port)])).unwrap();
                }
                b.query().await.send(reply(&[("b", b_port)])).unwrap();
            }
        );
        let sources = sources.unwrap();
        assert_eq!(sources.clusters.len(), 2);
        assert_eq!(sources.monitors().len(), 2);
    }

    #[tokio::test]
    async fn named_cluster_uses_its_settings_and_replica_selection() {
        use clap::Parser;
        let mut node = FakeNode::new().await;
        let port = node.port;
        let path = std::env::temp_dir().join(format!(
            "redis-monitor-topology-{}-{port}.toml",
            std::process::id()
        ));
        std::fs::write(&path, format!("[cache]\ncluster = true\naddresses = [\"127.0.0.1:{port}\"]\nuser = \"named\"\npass = \"secret\"\n")).unwrap();
        let config = crate::config::Map::load(Some(&path)).unwrap();
        std::fs::remove_file(path).unwrap();
        let options = crate::Options::try_parse_from([
            "redis-monitor",
            "--replicas",
            "cache",
            "cache",
        ])
        .unwrap();
        let auth = ServerAuth::from_user_pass(Some("cli"), None);
        let (sources, ()) = tokio::join!(
            crate::process_instances(&config, &options, None, &auth),
            async {
                node.query()
                    .await
                    .send(reply(&[("a", port), ("b", 1)]))
                    .unwrap();
            }
        );
        let sources = sources.unwrap();
        assert_eq!(sources.clusters.len(), 1);
        assert_eq!(sources.monitors().len(), 2);
        for monitor in sources.monitors() {
            assert_eq!(monitor.name.as_deref(), Some("cache"));
            assert_eq!(monitor.auth.user.as_deref(), Some("named"));
        }
    }

    #[tokio::test]
    async fn shutdown_cancels_initial_discovery() {
        use clap::Parser;
        let mut seed = FakeNode::new().await;
        let options = crate::Options::try_parse_from([
            "redis-monitor",
            "--cluster",
            &seed.address().to_string(),
        ])
        .unwrap();
        let (shutdown, _) = watch::channel(false);
        let running = tokio::spawn(crate::run_wire(options, shutdown.clone()));
        let _stalled = seed.query().await;
        shutdown.send_replace(true);
        within(running).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn closed_output_stops_the_supervisor_without_restarting_sources() {
        let mut node = FakeNode::new().await;
        let group = group(&[("a", node.port)], false);
        let (io, rx) = test_io(1, 4096);
        drop(rx);
        let pipeline = pipeline(io, BatchConfig::new(false));
        let (_shutdown, stop) = watch::channel(false);
        let running =
            tokio::spawn(run(group, Duration::from_secs(30), pipeline, stop));
        let mut stream = node.monitor().await;
        stream
            .write_all(b"+1.0 [0 lua] \"PING\"\r\n")
            .await
            .unwrap();
        within(running).await.unwrap().unwrap();
        assert!(node.monitors.try_recv().is_err());
        assert_eq!(within(stream.read(&mut [0])).await.unwrap(), 0);
    }
}

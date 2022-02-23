use std::sync::Arc;

use anyhow::Result;
use maplit::btreeset;
use openraft::raft::AppendEntriesRequest;
use openraft::Config;
use openraft::LeaderId;
use openraft::LogId;
use openraft::RaftNetwork;
use openraft::Vote;

use crate::fixtures::RaftRouter;

/// append-entries should update hard state when adding new logs with bigger term
///
/// - bring up a learner and send to it append_entries request. Check the hard state updated.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn append_entries_with_bigger_term() -> Result<()> {
    let (_log_guard, ut_span) = init_ut!();
    let _ent = ut_span.enter();

    // Setup test dependencies.
    let mut config = Arc::new(Config::default().validate()?);
    let config = Arc::get_mut(&mut config).unwrap();
    config.heartbeat_interval = 50_000;
    config.election_timeout_min = 150_000;
    config.election_timeout_max = 300_000;
    let router = Arc::new(RaftRouter::new(Arc::new((*config).clone())));
    println!("{}", "here0...");
    let log_index = router.new_nodes_from_single(btreeset! {0}, btreeset! {1}).await?;

    println!("{}", "here1...");
    // before append entries, check hard state in term 1 and vote for node 0
    router
        .assert_storage_state(1, log_index, Some(0), LogId::new(LeaderId::new(1, 0), log_index), None)
        .await?;

    println!("{}", "here2...");
    // append entries with term 2 and leader_id, this MUST cause hard state changed in node 0
    let req = AppendEntriesRequest::<memstore::ClientRequest> {
        vote: Vote::new(2, 1),
        prev_log_id: Some(LogId::new(LeaderId::new(1, 0), log_index)),
        entries: vec![],
        leader_commit: Some(LogId::new(LeaderId::new(1, 0), log_index)),
    };

    let resp = router.send_append_entries(0, None, req).await?;
    assert!(resp.success);

    // after append entries, check hard state in term 2 and vote for node 1
    router
        .assert_storage_state_in_node(
            0,
            2,
            log_index,
            Some(1),
            LogId::new(LeaderId::new(1, 0), log_index),
            None,
        )
        .await?;

    Ok(())
}

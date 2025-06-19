#![cfg(feature = "testing")]

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::testing::log_id; // Correct way to get LogId in tests
use openraft::ClientWriteRequest;
use openraft::Config;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId; // Keep direct LogId for clarity in assertions
use openraft::OnEntryCommitted;
use openraft::AppData;
use openraft::AppDataResponse as OpenRaftAppDataResponse; // Alias to avoid conflict if we name ours AppDataResponse

use crate::fixtures::RaftRouter;

// Define a simple AppData and AppDataResponse for the tests
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TestAppData {
    pub val: u64,
    pub membership_val: Option<openraft::Membership>,
}
impl AppData for TestAppData {}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TestAppDataResponse {
    pub val: Option<u64>,
}
impl OpenRaftAppDataResponse for TestAppDataResponse {} // Use aliased trait

// Mock OnEntryCommitted handler
#[derive(Clone)]
struct MockCommittedHandler {
    committed_entries: Arc<Mutex<Vec<Entry<TestAppData>>>>,
}

impl MockCommittedHandler {
    fn new() -> Self {
        Self {
            committed_entries: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl OnEntryCommitted<TestAppData> for MockCommittedHandler {
    fn on_entry_committed(&self, entry: &Entry<TestAppData>) {
        self.committed_entries.lock().unwrap().push(entry.clone());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_basic_commit_notification() -> Result<()> {
    let cfg = Arc::new(Config::default().validate()?);
    let mut router = RaftRouter::new(cfg.clone());

    router.new_raft_node(0).await;
    // Increased timeout for slower CI environments
    router.wait_for_leader(Duration::from_secs(3)).await?;
    let leader_id = router.leader().await.expect("leader not found");
    let leader_node = router.get_raft_handle(&leader_id)?;

    let handler = Arc::new(MockCommittedHandler::new());
    leader_node.add_on_entry_committed_handler(handler.clone());

    // Write an entry
    let client_req = ClientWriteRequest::new(TestAppData { val: 10, membership_val: None });
    let res = leader_node.client_write(client_req).await?;

    // Leader election commits a blank log at index 1 in term 1.
    // The first client write will be at index 2 in term 1.
    let expected_written_log_id = log_id(1, 2); // Use testing::log_id for construction
    assert_eq!(res.log_id, expected_written_log_id, "Written log ID mismatch");

    // Wait for the log entry to be committed on the leader.
    router.wait_for_log(&btreeset!{leader_id}, expected_written_log_id, Duration::from_secs(3), "wait for client write to commit").await?;

    let committed_entries_locked = handler.committed_entries.lock().unwrap();
    assert_eq!(committed_entries_locked.len(), 1, "Expected one entry notification for the specific write");

    let notified_entry = &committed_entries_locked[0];
    assert_eq!(notified_entry.log_id, expected_written_log_id, "Notified entry LogId mismatch");
    if let EntryPayload::Normal(data) = &notified_entry.payload {
        assert_eq!(data.val, 10, "Notified entry data mismatch");
    } else {
        panic!("Expected normal entry payload, got {:?}", notified_entry.payload);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multiple_handlers_commit_notification() -> Result<()> {
    let cfg = Arc::new(Config::default().validate()?);
    let mut router = RaftRouter::new(cfg.clone());

    router.new_raft_node(0).await;
    router.wait_for_leader(Duration::from_secs(3)).await?;
    let leader_id = router.leader().await.expect("leader not found");
    let leader_node = router.get_raft_handle(&leader_id)?;

    let handler1 = Arc::new(MockCommittedHandler::new());
    let handler2 = Arc::new(MockCommittedHandler::new());
    leader_node.add_on_entry_committed_handler(handler1.clone());
    leader_node.add_on_entry_committed_handler(handler2.clone());

    let client_req = ClientWriteRequest::new(TestAppData { val: 20, membership_val: None });
    let res = leader_node.client_write(client_req).await?;
    let expected_written_log_id = log_id(1, 2); // term 1, index 2 (after initial blank log)
    assert_eq!(res.log_id, expected_written_log_id);

    router.wait_for_log(&btreeset!{leader_id}, expected_written_log_id, Duration::from_secs(3), "wait for client write to commit").await?;

    let committed1 = handler1.committed_entries.lock().unwrap();
    assert_eq!(committed1.len(), 1, "Handler 1: Expected one entry");
    assert_eq!(committed1[0].log_id, expected_written_log_id);
    if let EntryPayload::Normal(data) = &committed1[0].payload {
        assert_eq!(data.val, 20);
    } else {
        panic!("Handler 1: Expected normal entry payload");
    }

    let committed2 = handler2.committed_entries.lock().unwrap();
    assert_eq!(committed2.len(), 1, "Handler 2: Expected one entry");
    assert_eq!(committed2[0].log_id, expected_written_log_id);
    if let EntryPayload::Normal(data) = &committed2[0].payload {
        assert_eq!(data.val, 20);
    } else {
        panic!("Handler 2: Expected normal entry payload");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_no_handlers_commit_notification() -> Result<()> {
    let cfg = Arc::new(Config::default().validate()?);
    let mut router = RaftRouter::new(cfg.clone());

    router.new_raft_node(0).await;
    router.wait_for_leader(Duration::from_secs(3)).await?;
    let leader_id = router.leader().await.expect("leader not found");
    let leader_node = router.get_raft_handle(&leader_id)?;

    // No handlers added

    let client_req = ClientWriteRequest::new(TestAppData { val: 30, membership_val: None });
    let res = leader_node.client_write(client_req).await?;
    let expected_written_log_id = log_id(1, 2);
    assert_eq!(res.log_id, expected_written_log_id);

    // Verify the write still succeeds and is committed by waiting for the log
    router.wait_for_log(&btreeset!{leader_id}, expected_written_log_id, Duration::from_secs(3), "wait for client write to commit").await?;

    // Main check is that no panics occurred and the operation completed successfully.
    // If wait_for_log passes, the log is there and committed.
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_commit_notification_for_blank_entry_on_leader_election() -> Result<()> {
    let cfg = Arc::new(Config::default().validate()?);
    let mut router = RaftRouter::new(cfg.clone());

    // Create node 0, it will become leader. Add handler before leader election fully completes.
    let node_0 = router.new_raft_node(0).await;
    let handler = Arc::new(MockCommittedHandler::new());
    node_0.add_on_entry_committed_handler(handler.clone());

    router.wait_for_leader(Duration::from_secs(3)).await?; // Node 0 becomes leader and commits blank log

    let expected_blank_log_id = log_id(1, 1); // First log by the new leader

    // Wait for the blank log to be processed by the handler
    // We can use wait_for_log on the leader itself (node_0)
    router.wait_for_log(&btreeset!{0}, expected_blank_log_id, Duration::from_secs(3), "wait for initial blank log commit").await?;

    let committed_entries = handler.committed_entries.lock().unwrap();

    let mut found_blank_entry = false;
    for entry in committed_entries.iter() {
        if entry.log_id == expected_blank_log_id {
            if matches!(entry.payload, EntryPayload::Blank) {
                found_blank_entry = true;
                break;
            }
        }
    }
    assert!(found_blank_entry, "Expected blank entry (log_id {:?}) notification, got: {:?}", expected_blank_log_id, committed_entries);

    Ok(())
}

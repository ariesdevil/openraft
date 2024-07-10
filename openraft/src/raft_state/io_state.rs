use crate::display_ext::DisplayOption;
use crate::raft_state::io_state::append_log_io_id::AppendLogIOId;
use crate::LogId;
use crate::RaftTypeConfig;
use crate::Vote;

pub(crate) mod append_log_io_id;
pub(crate) mod io_id;

/// IOState tracks the state of actually happened io including log flushed, applying log to state
/// machine or snapshot building.
///
/// These states are updated only when the io complete and thus may fall behind to the state stored
/// in [`RaftState`](`crate::RaftState`),.
///
/// The log ids that are tracked includes:
///
/// ```text
/// | log ids
/// | *------------+---------+---------+---------+------------------>
/// |              |         |         |         `---> flushed
/// |              |         |         `-------------> applied
/// |              |         `-----------------------> snapshot
/// |              `---------------------------------> purged
/// ```
#[derive(Debug, Clone, Copy)]
#[derive(Default)]
#[derive(PartialEq, Eq)]
pub(crate) struct IOState<C>
where C: RaftTypeConfig
{
    /// Whether it is building a snapshot
    building_snapshot: bool,

    /// The last flushed vote.
    pub(crate) vote: Vote<C::NodeId>,

    /// The last log id that has been flushed to storage.
    // TODO: this wont be used until we move log io into separate task.
    pub(crate) flushed: Option<AppendLogIOId<C>>,

    /// The last log id that has been applied to state machine.
    pub(crate) applied: Option<LogId<C::NodeId>>,

    /// The last log id in the currently persisted snapshot.
    pub(crate) snapshot: Option<LogId<C::NodeId>>,

    /// The last log id that has been purged from storage.
    ///
    /// `RaftState::last_purged_log_id()`
    /// is just the log id that is going to be purged, i.e., there is a `PurgeLog` command queued to
    /// be executed, and it may not be the actually purged log id.
    pub(crate) purged: Option<LogId<C::NodeId>>,
}

impl<C> IOState<C>
where C: RaftTypeConfig
{
    pub(crate) fn new(
        vote: Vote<C::NodeId>,
        applied: Option<LogId<C::NodeId>>,
        snapshot: Option<LogId<C::NodeId>>,
        purged: Option<LogId<C::NodeId>>,
    ) -> Self {
        Self {
            building_snapshot: false,
            vote,
            flushed: None,
            applied,
            snapshot,
            purged,
        }
    }

    pub(crate) fn update_vote(&mut self, vote: Vote<C::NodeId>) {
        self.vote = vote;
    }

    pub(crate) fn vote(&self) -> &Vote<C::NodeId> {
        &self.vote
    }

    pub(crate) fn update_applied(&mut self, log_id: Option<LogId<C::NodeId>>) {
        tracing::debug!(applied = display(DisplayOption(&log_id)), "{}", func_name!());

        // TODO: should we update flushed if applied is newer?
        debug_assert!(
            log_id > self.applied,
            "applied log id should be monotonically increasing: current: {:?}, update: {:?}",
            self.applied,
            log_id
        );

        self.applied = log_id;
    }

    pub(crate) fn applied(&self) -> Option<&LogId<C::NodeId>> {
        self.applied.as_ref()
    }

    pub(crate) fn update_snapshot(&mut self, log_id: Option<LogId<C::NodeId>>) {
        tracing::debug!(snapshot = display(DisplayOption(&log_id)), "{}", func_name!());

        debug_assert!(
            log_id > self.snapshot,
            "snapshot log id should be monotonically increasing: current: {:?}, update: {:?}",
            self.snapshot,
            log_id
        );

        self.snapshot = log_id;
    }

    pub(crate) fn snapshot(&self) -> Option<&LogId<C::NodeId>> {
        self.snapshot.as_ref()
    }

    pub(crate) fn set_building_snapshot(&mut self, building: bool) {
        self.building_snapshot = building;
    }

    pub(crate) fn building_snapshot(&self) -> bool {
        self.building_snapshot
    }

    pub(crate) fn update_purged(&mut self, log_id: Option<LogId<C::NodeId>>) {
        self.purged = log_id;
    }

    pub(crate) fn purged(&self) -> Option<&LogId<C::NodeId>> {
        self.purged.as_ref()
    }
}

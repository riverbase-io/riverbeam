//! Execution statuses and legal transitions (port of fluvius-beam `execution/status.py`).

use beam_store::DbEnum;
use serde::{Deserialize, Serialize};

/// `agent_execution.status`. Wire value is lower snake case; the Postgres label is upper case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Created,
    Queued,
    Running,
    WaitingForInput,
    Retrying,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
}

impl ExecutionStatus {
    pub const ALL: &'static [Self] = &[
        Self::Created,
        Self::Queued,
        Self::Running,
        Self::WaitingForInput,
        Self::Retrying,
        Self::Completed,
        Self::Failed,
        Self::Cancelled,
        Self::TimedOut,
    ];

    /// Python `ExecutionStatus` value (`"waiting_for_input"`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Queued => "queued",
            Self::Running => "running",
            Self::WaitingForInput => "waiting_for_input",
            Self::Retrying => "retrying",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
        }
    }

    /// `TERMINAL_STATUSES`.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }

    /// `BLOCKING_CONCURRENCY_STATUSES`: an execution in these holds its concurrency slot.
    #[must_use]
    pub fn blocks_concurrency(self) -> bool {
        matches!(
            self,
            Self::Created | Self::Queued | Self::Running | Self::Retrying | Self::WaitingForInput
        )
    }

    /// Statuses `self` may move to (`_ALLOWED`).
    #[must_use]
    pub fn allowed_targets(self) -> &'static [Self] {
        use ExecutionStatus::{
            Cancelled, Completed, Failed, Queued, Retrying, Running, TimedOut, WaitingForInput,
        };
        match self {
            Self::Created => &[Queued, Running, Cancelled],
            Self::Queued => &[Running, Cancelled, TimedOut],
            Self::Running => &[
                WaitingForInput,
                Completed,
                Failed,
                Cancelled,
                TimedOut,
                Retrying,
            ],
            Self::WaitingForInput => &[Running, Cancelled, TimedOut, Failed],
            Self::Retrying => &[Running, Failed, Cancelled, TimedOut],
            Self::Failed => &[Retrying],
            Self::Completed | Self::Cancelled | Self::TimedOut => &[],
        }
    }
}

impl DbEnum for ExecutionStatus {
    const PG_TYPE: &'static str = "executionstatus";

    fn db_label(self) -> &'static str {
        match self {
            Self::Created => "CREATED",
            Self::Queued => "QUEUED",
            Self::Running => "RUNNING",
            Self::WaitingForInput => "WAITING_FOR_INPUT",
            Self::Retrying => "RETRYING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::TimedOut => "TIMED_OUT",
        }
    }

    fn from_db_label(label: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|s| s.db_label() == label)
    }
}

/// `target` when legal, else `BEM-370` (public `BEM-425`).
///
/// # Errors
/// Returns `BEM-370` when the move is not in the transition table.
pub fn transition(
    current: ExecutionStatus,
    target: ExecutionStatus,
) -> riverbase_core::RiverbaseResult<ExecutionStatus> {
    if current.allowed_targets().contains(&target) {
        Ok(target)
    } else {
        Err(crate::BEM_370.with_data(format!("{} -> {}", current.as_str(), target.as_str())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Python `_ALLOWED`, verbatim (fluvius-beam `execution/status.py` at the plan baseline).
    const PYTHON_ALLOWED: &[(&str, &[&str])] = &[
        ("created", &["queued", "running", "cancelled"]),
        ("queued", &["running", "cancelled", "timed_out"]),
        (
            "running",
            &[
                "waiting_for_input",
                "completed",
                "failed",
                "cancelled",
                "timed_out",
                "retrying",
            ],
        ),
        (
            "waiting_for_input",
            &["running", "cancelled", "timed_out", "failed"],
        ),
        ("retrying", &["running", "failed", "cancelled", "timed_out"]),
        ("failed", &["retrying"]),
    ];

    #[test]
    fn transition_table_matches_python() {
        for &from in ExecutionStatus::ALL {
            let allowed: &[&str] = PYTHON_ALLOWED
                .iter()
                .find(|(f, _)| *f == from.as_str())
                .map_or(&[], |(_, to)| to);
            for &to in ExecutionStatus::ALL {
                let legal = allowed.contains(&to.as_str());
                assert_eq!(
                    transition(from, to).is_ok(),
                    legal,
                    "{} -> {}",
                    from.as_str(),
                    to.as_str()
                );
                if !legal {
                    assert_eq!(
                        transition(from, to).unwrap_err().errcode.as_str(),
                        "BEM-370"
                    );
                }
            }
        }
    }

    #[test]
    fn labels_match_golden_schema() {
        let golden = include_str!("../../../tests/schema/python_ref_beam.sql");
        let head = "CREATE TYPE ref_beam.executionstatus AS ENUM (";
        let body = &golden[golden.find(head).unwrap() + head.len()..];
        let labels: Vec<&str> = body[..body.find(");").unwrap()]
            .split(',')
            .map(|l| l.trim().trim_matches('\''))
            .collect();
        let ours: Vec<&str> = ExecutionStatus::ALL.iter().map(|s| s.db_label()).collect();
        assert_eq!(ours, labels);
    }

    #[test]
    fn terminal_and_blocking_sets() {
        let terminal: Vec<_> = ExecutionStatus::ALL
            .iter()
            .filter(|s| s.is_terminal())
            .map(|s| s.as_str())
            .collect();
        assert_eq!(terminal, ["completed", "failed", "cancelled", "timed_out"]);
        for s in ExecutionStatus::ALL {
            assert_eq!(s.blocks_concurrency(), !s.is_terminal(), "{}", s.as_str());
        }
    }
}

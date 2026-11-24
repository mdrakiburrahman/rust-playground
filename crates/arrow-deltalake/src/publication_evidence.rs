use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};

/// Evidence for one exact application ID and application version. The versions
/// carried by the first two variants are Delta log versions, not app versions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicationOutcome {
    CommittedNow(u64),
    AlreadyCommitted(u64),
    Unknown(PublicationReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationReason {
    NotSubmitted,
    NoCommitProvenance,
    Deduplicated,
    PartialGroup,
    Unidentifiable,
    Fenced,
    Conflict,
    Retry,
    RetryableIo,
    Reconciled,
    SyncFailed,
    Error,
}

/// Evidence from the entire `publish_files` call, not just its final attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicationEvidence {
    pub outcomes: BTreeMap<(String, i64), PublicationOutcome>,
    pub clean: bool,
    pub reasons: BTreeSet<PublicationReason>,
}

impl PublicationEvidence {
    pub(crate) fn new<'a>(ids: impl Iterator<Item = (&'a str, i64)>) -> Self {
        Self {
            outcomes: ids
                .map(|(id, v)| {
                    (
                        (id.to_owned(), v),
                        PublicationOutcome::Unknown(PublicationReason::NotSubmitted),
                    )
                })
                .collect(),
            reasons: BTreeSet::new(),
            clean: true,
        }
    }

    pub(crate) fn mark(&mut self, reason: PublicationReason) {
        self.clean = false;
        self.reasons.insert(reason);
    }

    pub(crate) fn fail(&mut self, reason: PublicationReason) {
        self.mark(reason);
        for outcome in self.outcomes.values_mut() {
            if matches!(outcome, PublicationOutcome::Unknown(PublicationReason::NotSubmitted))
                || reason == PublicationReason::SyncFailed
            {
                *outcome = PublicationOutcome::Unknown(reason);
            }
        }
    }
}

impl Display for PublicationEvidence {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "publication evidence: {:?}", self.reasons)
    }
}

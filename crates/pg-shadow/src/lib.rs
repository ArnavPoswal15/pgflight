//! Shadow EXPLAIN runner, `.pgrec` replayer, and plan heuristics (ADR-003).

mod compare;
mod explain;
mod guard;
mod heuristics;
mod replayer;

pub use compare::{compare_plans, PlanCompareSummary, PlanDiffNode};
pub use explain::{
    ExplainError, ExplainOutput, ShadowClient, DEFAULT_SHADOW_URL, DEFAULT_STATEMENT_TIMEOUT_MS,
};
pub use guard::{assert_select_only, GuardError};
pub use heuristics::{HeuristicConfig, PlanHint, PlanHints};
pub use replayer::{query_pgrec_record, types_from_oids, ReplayError, ReplayStats, Replayer};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compare_removes_seq_scan() {
        let baseline = json!([{
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "users",
                "Plans": []
            }
        }]);
        let variant = json!([{
            "Plan": {
                "Node Type": "Index Scan",
                "Relation Name": "users",
                "Plans": []
            }
        }]);
        let cmp = compare_plans(&baseline, &variant);
        assert!(!cmp.improved.is_empty());
    }
}

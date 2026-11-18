//! Structural diff of two EXPLAIN JSON plans (replay A/B).

use crate::heuristics::collect_plan_signatures;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanDiffNode {
    pub path: String,
    pub baseline: Option<String>,
    pub variant: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanCompareSummary {
    pub improved: Vec<String>,
    pub regressed: Vec<String>,
    pub diff_nodes: Vec<PlanDiffNode>,
}

const LESS_DESIRABLE: &[&str] = &["Seq Scan", "Bitmap Heap Scan"];
const MORE_DESIRABLE: &[&str] = &["Index Scan", "Index Only Scan", "Bitmap Index Scan"];

/// Compare baseline (A) and variant (B) EXPLAIN JSON payloads.
pub fn compare_plans(baseline: &Value, variant: &Value) -> PlanCompareSummary {
    let base_sigs = collect_plan_signatures(baseline);
    let var_sigs = collect_plan_signatures(variant);
    let mut diff_nodes = Vec::new();
    let len = base_sigs.len().max(var_sigs.len());
    let mut improved = Vec::new();
    let mut regressed = Vec::new();
    for i in 0..len {
        let (b_path, b_type, b_rel) =
            base_sigs
                .get(i)
                .cloned()
                .unwrap_or((format!("Plan/?[{i}]"), "?".into(), None));
        let (_v_path, v_type, v_rel) =
            var_sigs
                .get(i)
                .cloned()
                .unwrap_or((format!("Plan/?[{i}]"), "?".into(), None));
        if b_type != v_type {
            diff_nodes.push(PlanDiffNode {
                path: b_path.clone(),
                baseline: Some(b_type.clone()),
                variant: Some(v_type.clone()),
            });
            let rel = b_rel.as_deref().or(v_rel.as_deref()).unwrap_or("query");
            if node_improved(&b_type, &v_type) {
                improved.push(format!("{rel}: {b_type} → {v_type} (improved)"));
            } else if node_regressed(&b_type, &v_type) {
                regressed.push(format!("{rel}: {b_type} → {v_type} (regressed)"));
            }
        }
    }
    PlanCompareSummary {
        improved,
        regressed,
        diff_nodes,
    }
}

fn node_improved(baseline: &str, variant: &str) -> bool {
    LESS_DESIRABLE.contains(&baseline) && MORE_DESIRABLE.contains(&variant)
}

fn node_regressed(baseline: &str, variant: &str) -> bool {
    MORE_DESIRABLE.contains(&baseline) && LESS_DESIRABLE.contains(&variant)
}

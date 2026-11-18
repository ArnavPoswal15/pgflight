//! EXPLAIN JSON plan heuristics (FEATURES §6).

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlanHint {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_path: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanHints {
    pub hints: Vec<PlanHint>,
}

#[derive(Debug, Clone)]
pub struct HeuristicConfig {
    /// Seq Scan total rows (`Actual Rows * Actual Loops`) above this → missing index hint.
    pub seq_scan_rows: u64,
    /// `actual / max(plan, 1)` above this → stale stats hint.
    pub stale_stats_ratio: f64,
}

impl Default for HeuristicConfig {
    fn default() -> Self {
        Self {
            seq_scan_rows: 5000,
            stale_stats_ratio: 10.0,
        }
    }
}

impl PlanHints {
    /// Scan a Postgres `EXPLAIN (FORMAT JSON)` payload for performance hints.
    pub fn from_json(plan: &Value) -> Self {
        Self::from_json_with_config(plan, &HeuristicConfig::default())
    }

    pub fn from_json_with_config(plan: &Value, cfg: &HeuristicConfig) -> Self {
        let tree = plan_tree(plan);
        let mut hints = Vec::new();
        walk_plan(&tree, "Plan", cfg, &mut hints);
        PlanHints { hints }
    }
}

fn plan_root(plan: &Value) -> Value {
    if let Some(arr) = plan.as_array() {
        if let Some(first) = arr.first() {
            return first.clone();
        }
    }
    plan.clone()
}

fn plan_tree(plan: &Value) -> Value {
    let owned = plan_root(plan);
    if let Some(inner) = owned.get("Plan") {
        return inner.clone();
    }
    if let Some(inner) = plan.get("Plan") {
        return inner.clone();
    }
    owned
}

#[derive(Debug, Clone)]
struct PlanNode {
    path: String,
    node_type: String,
    relation: Option<String>,
    actual_rows: f64,
    plan_rows: f64,
    actual_loops: f64,
    sort_space_type: Option<String>,
}

fn walk_plan(value: &Value, path: &str, cfg: &HeuristicConfig, out: &mut Vec<PlanHint>) {
    let node = scan_node(value, path);
    apply_node_heuristics(&node, cfg, out);
    if let Some(children) = value.get("Plans").and_then(Value::as_array) {
        for (i, child) in children.iter().enumerate() {
            let child_path = format!("{}/Plans[{i}]", path);
            walk_plan(child, &child_path, cfg, out);
        }
    }
}

fn scan_node(value: &Value, path: &str) -> PlanNode {
    PlanNode {
        path: path.to_string(),
        node_type: value
            .get("Node Type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        relation: value
            .get("Relation Name")
            .and_then(Value::as_str)
            .map(str::to_string),
        actual_rows: json_f64(value.get("Actual Rows")),
        plan_rows: json_f64(value.get("Plan Rows")),
        actual_loops: json_f64(value.get("Actual Loops")).max(1.0),
        sort_space_type: value
            .get("Sort Space Type")
            .and_then(Value::as_str)
            .map(str::to_string),
    }
}

fn apply_node_heuristics(node: &PlanNode, cfg: &HeuristicConfig, out: &mut Vec<PlanHint>) {
    if node.node_type == "Seq Scan" {
        let total_rows = (node.actual_rows * node.actual_loops).round() as u64;
        if total_rows > cfg.seq_scan_rows {
            let rel = node.relation.as_deref().unwrap_or("<unknown>");
            out.push(PlanHint {
                code: "missing_index".into(),
                message: format!(
                    "Seq Scan on `{rel}` read ~{total_rows} rows (threshold {})",
                    cfg.seq_scan_rows
                ),
                node_path: Some(node.path.clone()),
            });
        }
    }
    if node.sort_space_type.as_deref() == Some("Disk") {
        out.push(PlanHint {
            code: "sort_spill".into(),
            message: "Sort spilled to disk — consider raising work_mem".into(),
            node_path: Some(node.path.clone()),
        });
    }
    let plan = node.plan_rows.max(1.0);
    let actual = node.actual_rows * node.actual_loops;
    if actual > plan * cfg.stale_stats_ratio {
        out.push(PlanHint {
            code: "stale_stats".into(),
            message: format!("Actual rows (~{actual:.0}) much higher than plan rows ({plan:.0})"),
            node_path: Some(node.path.clone()),
        });
    }
}

fn json_f64(v: Option<&Value>) -> f64 {
    match v {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        Some(Value::String(s)) => s.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

/// Collect `(path, node_type, relation)` for structural compare.
pub(crate) fn collect_plan_signatures(plan: &Value) -> Vec<(String, String, Option<String>)> {
    let tree = plan_tree(plan);
    let mut out = Vec::new();
    sig_walk(&tree, "Plan", &mut out);
    out
}

fn sig_walk(value: &Value, path: &str, out: &mut Vec<(String, String, Option<String>)>) {
    let node_type = value
        .get("Node Type")
        .and_then(Value::as_str)
        .unwrap_or("?")
        .to_string();
    let relation = value
        .get("Relation Name")
        .and_then(Value::as_str)
        .map(str::to_string);
    out.push((path.to_string(), node_type.clone(), relation));
    if let Some(children) = value.get("Plans").and_then(Value::as_array) {
        for (i, child) in children.iter().enumerate() {
            sig_walk(child, &format!("{path}/Plans[{i}]"), out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn detects_seq_scan_missing_index() {
        let plan = json!([{
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "users",
                "Actual Rows": 9000,
                "Actual Loops": 1,
                "Plan Rows": 100
            }
        }]);
        let hints = PlanHints::from_json(&plan);
        assert!(hints.hints.iter().any(|h| h.code == "missing_index"));
    }

    #[test]
    fn detects_sort_spill() {
        let plan = json!([{
            "Plan": {
                "Node Type": "Sort",
                "Sort Space Type": "Disk",
                "Actual Rows": 10,
                "Plan Rows": 10
            }
        }]);
        let hints = PlanHints::from_json(&plan);
        assert!(hints.hints.iter().any(|h| h.code == "sort_spill"));
    }

    #[test]
    fn detects_stale_stats() {
        let plan = json!([{
            "Plan": {
                "Node Type": "Index Scan",
                "Actual Rows": 5000,
                "Actual Loops": 1,
                "Plan Rows": 10
            }
        }]);
        let hints = PlanHints::from_json(&plan);
        assert!(hints.hints.iter().any(|h| h.code == "stale_stats"));
    }

    #[test]
    fn small_seq_scan_no_hint() {
        let plan = json!([{
            "Plan": {
                "Node Type": "Seq Scan",
                "Relation Name": "users",
                "Actual Rows": 10,
                "Plan Rows": 10
            }
        }]);
        let hints = PlanHints::from_json(&plan);
        assert!(!hints.hints.iter().any(|h| h.code == "missing_index"));
    }
}

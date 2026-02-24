from scripts.sync_pipedrive_to_notion_opportunities import evaluate_gate


def test_gate_pass_and_rollback():
    rules = {
        "gates": {
            "Estimation": ["brief", "scope"],
            "Validation": ["estimate"],
        },
        "hard_rollback": True,
        "rollback_reason_template": "Blocked move to {target_stage}: missing {missing}",
    }
    order = ["Future pipeline", "Scope Definition", "Estimation", "Validation", "Presented"]

    final_stage, reason = evaluate_gate("Estimation", {"brief": True, "scope": True}, rules, order)
    assert final_stage == "Estimation"
    assert reason is None

    final_stage, reason = evaluate_gate("Estimation", {"brief": True, "scope": False}, rules, order)
    assert final_stage == "Scope Definition"
    assert reason == "Blocked move to Estimation: missing scope"

    final_stage, reason = evaluate_gate("Validation", {"estimate": False}, rules, order)
    assert final_stage == "Estimation"
    assert "missing estimate" in reason

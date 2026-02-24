from scripts.sync_pipedrive_to_notion_opportunities import dedupe_by_deal_id, plan_upsert_actions


def test_upsert_plan_idempotent_with_duplicates():
    deals = [
        {"id": 1, "update_time": "2026-02-01 10:00:00"},
        {"id": 1, "update_time": "2026-02-02 10:00:00"},
        {"id": 2, "update_time": "2026-02-01 10:00:00"},
    ]
    deduped = dedupe_by_deal_id(deals)
    assert len(deduped) == 2

    existing = {1: {"id": "page_1"}}
    actions = plan_upsert_actions(deals, existing)
    assert actions["updates"] == 1
    assert actions["creates"] == 1

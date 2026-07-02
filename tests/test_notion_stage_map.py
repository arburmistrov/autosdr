from scripts.sync_pipedrive_to_notion_opportunities import map_stage


def test_stage_map_explicit_and_preop():
    cfg = {
        "pre_opportunity_target": "Future pipeline",
        "pre_opportunity_stage_names": ["Company Longlist", "Replied"],
        "explicit_map": {
            "Opportunity": "Opportunity",
            "Won": "Won",
        },
    }

    assert map_stage("Opportunity", cfg) == "Opportunity"
    assert map_stage("Replied", cfg) == "Future pipeline"
    assert map_stage("Unknown Stage", cfg) == "Future pipeline"

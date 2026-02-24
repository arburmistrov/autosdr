from scripts.sync_pipedrive_to_notion_opportunities import compute_sla_color


def test_sla_color_boundaries():
    assert compute_sla_color(0) == "Green"
    assert compute_sla_color(3) == "Green"
    assert compute_sla_color(4) == "Yellow"
    assert compute_sla_color(7) == "Yellow"
    assert compute_sla_color(8) == "Red"

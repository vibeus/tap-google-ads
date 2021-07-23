from .campaigns import Campaigns
from .campaign_metrics import CampaignMetrics


def create_stream(stream_id):
    if stream_id == "campaigns":
        return Campaigns()
    elif stream_id == "campaign_metrics":
        return CampaignMetrics()

    assert False, f"Unsupported stream: {stream_id}"

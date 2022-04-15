from .campaigns import Campaigns
from .campaign_metrics import CampaignMetrics
from .ad_groups import AdGroups
from .ad_group_metrics import AdGroupMetrics


def create_stream(stream_id):
    if stream_id == "campaigns":
        return Campaigns()
    elif stream_id == "campaign_metrics":
        return CampaignMetrics()
    elif stream_id == "ad_groups":
        return AdGroups()
    elif stream_id == "ad_group_metrics":
        return AdGroupMetrics()

    assert False, f"Unsupported stream: {stream_id}"

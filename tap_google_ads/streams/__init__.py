from .campaigns import Campaigns
from .campaign_metrics import CampaignMetrics
from .campaign_metrics_conversions import CampaignMetricsConversions
from .ad_groups import AdGroups
from .ad_group_metrics import AdGroupMetrics
from .ad_group_metrics_conversions import AdGroupMetricsConversions


def create_stream(stream_id):
    if stream_id == "campaigns":
        return Campaigns()
    elif stream_id == "campaign_metrics":
        return CampaignMetrics()
    elif stream_id == "campaign_metrics_conversions":
        return CampaignMetricsConversions()
    elif stream_id == "ad_groups":
        return AdGroups()
    elif stream_id == "ad_group_metrics":
        return AdGroupMetrics()
    elif stream_id == "ad_group_metrics_conversions":
        return AdGroupMetricsConversions()

    assert False, f"Unsupported stream: {stream_id}"

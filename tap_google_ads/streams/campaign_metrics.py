import singer
import json
import time
from singer import metadata
from datetime import datetime, timedelta
from dateutil.parser import parse
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from .base import Incremental
LOOKBACK_WINDOW = 30

LOGGER = singer.get_logger()

class CampaignMetrics(Incremental):
    @property
    def name(self):
        return "campaign_metrics"

    @property
    def key_properties(self):
        return ["campaign_id", "ad_network_type", "date", "device"]

    @property
    def replication_key(self):
        return "date"

    @property
    def replication_method(self):
        return "INCREMENTAL"

    def gen_records(self, config, service, customer_id):
        today = datetime.utcnow().date().isoformat()
        state_date = self._state.get(customer_id, self._start_date)
        after = max(self._start_date, state_date)
        start = (parse(after) - timedelta(days=LOOKBACK_WINDOW)).strftime("%Y-%m-%d")
        max_rep_key = after

        query = f"""
            SELECT
                campaign.id,
                segments.ad_network_type,
                segments.date,
                segments.device,
                metrics.all_conversions,
                metrics.clicks,
                metrics.conversions,
                metrics.cost_micros,
                metrics.cross_device_conversions,
                metrics.engagements,
                metrics.impressions,
                metrics.interactions
            FROM campaign
            WHERE segments.date >= '{start}' AND segments.date <= '{today}'
            """
        resp = service.search_stream(customer_id=customer_id, query=query)

        for batch in resp:
            for row in batch.results:
                s = row.segments
                c = row.campaign
                m = row.metrics

                rep_key = s.date
                if rep_key and rep_key > max_rep_key:
                    max_rep_key = rep_key

                yield {
                    "campaign_id": c.id,
                    "ad_network_type": s.ad_network_type,
                    "date": s.date,
                    "device": s.device,
                    "all_conversions": m.all_conversions,
                    "clicks": m.clicks,
                    "conversions": m.conversions,
                    "cost_micros": m.cost_micros,
                    "cross_device_conversions": m.cross_device_conversions,
                    "engagements": m.engagements,
                    "impressions": m.impressions,
                    "interactions": m.interactions,
                }

        self._state[customer_id] = max_rep_key

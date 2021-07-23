import singer
import json
import time
from singer import metadata
from datetime import datetime, timedelta
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

LOGGER = singer.get_logger()
CLIENT_CONFIG_KEYS = ["developer_token", "client_id", "client_secret", "refresh_token", "login_customer_id"]
DEFAULT_BACKOFF_SECONDS = 60


class CampaignMetrics:
    def __init__(self):
        self.__start_date = ""
        self.__state = {}

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

    @property
    def state(self):
        return self.__state

    def get_metadata(self, schema):
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=self.key_properties,
            valid_replication_keys=[self.replication_key],
            replication_method=self.replication_method,
        )

        return mdata

    def get_tap_data(self, config, state):
        today = datetime.utcnow().date().isoformat()
        self.__start_date = config.get("start_date", today)
        self.__backoff_seconds = config.get("rate_limit_backoff_seconds", DEFAULT_BACKOFF_SECONDS)
        self.__state = state.copy()

        client_config = {key: value for key, value in config.items() if key in CLIENT_CONFIG_KEYS}
        client = GoogleAdsClient.load_from_dict(config_dict=client_config, version="v8")
        service = client.get_service("GoogleAdsService")

        for customer_id in config["customer_ids"]:
            yield from self.get_campaign_metrics(config, service, customer_id)

    def get_campaign_metrics(self, config, service, customer_id):
        today = datetime.utcnow().date().isoformat()
        state_date = self.__state.get(customer_id, self.__start_date)
        after = max(self.__start_date, state_date)
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
            WHERE segments.date >= '{after}' AND segments.date <= '{today}'
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

        self.__state[customer_id] = max_rep_key

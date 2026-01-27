import singer
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
from .base import Incremental
LOOKBACK_WINDOW = 90

LOGGER = singer.get_logger()

class PmaxMetricsConversions(Incremental):
    @property
    def name(self):
        return "pmax_metrics_conversions"

    @property
    def key_properties(self):
        return ["campaign_id", "asset_id", "ad_network_type", "date", "device", "conversion_action", "conversion_action_name"]

    @property
    def replication_key(self):
        return "date"

    @property
    def replication_method(self):
        return "INCREMENTAL"

    def gen_records(self, config, service, customer_id):
        today = datetime.now(timezone.utc).date().isoformat()
        state_date = self._state.get(customer_id, self._start_date)
        after = max(self._start_date, state_date)
        start = (parse(after) - timedelta(days=LOOKBACK_WINDOW)).strftime("%Y-%m-%d")
        end_date = config.get("end_date", today)
        end = min(parse(after) + timedelta(days=365), parse(end_date)).strftime("%Y-%m-%d")
        max_rep_key = after

        LOGGER.info(f"{self.name}:{start} to {end}")

        query = f"""
            SELECT
                campaign.id,
                campaign.advertising_channel_type,
                asset.id,
                asset.name,
                asset.type,
                segments.ad_network_type,
                segments.date,
                segments.device,
                segments.conversion_action,
                segments.conversion_action_name,
                metrics.all_conversions,
                metrics.all_conversions_value,
                metrics.conversions,
                metrics.conversions_value,
                metrics.cross_device_conversions
            FROM campaign_asset
            WHERE campaign.advertising_channel_type = PERFORMANCE_MAX AND segments.date >= '{start}' AND segments.date <= '{end}'
            """
        resp = service.search_stream(customer_id=customer_id, query=query)

        for batch in resp:
            for row in batch.results:
                a = row.asset
                s = row.segments
                c = row.campaign
                m = row.metrics

                rep_key = s.date
                if rep_key and rep_key > max_rep_key:
                    max_rep_key = rep_key

                yield {
                    "campaign_id": c.id,
                    "asset_id": a.id,
                    "asset_name": a.name,
                    "asset_type": a.type_,
                    "advertising_channel_type": c.advertising_channel_type,
                    "ad_network_type": s.ad_network_type,
                    "date": s.date,
                    "device": s.device,
                    "conversion_action": s.conversion_action,
                    "conversion_action_name": s.conversion_action_name,
                    "all_conversions": m.all_conversions,
                    "all_conversions_value": m.all_conversions_value,
                    "conversions": m.conversions,
                    "conversions_value": m.conversions_value,
                    "cross_device_conversions": m.cross_device_conversions,
                    "customer_id": customer_id,
                }

        self._state[customer_id] = max_rep_key
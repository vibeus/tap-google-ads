import singer
import json
import time
from singer import metadata
from datetime import datetime, timedelta
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from .base import Base

LOGGER = singer.get_logger()

class AdGroups(Base):
    @property
    def name(self):
        return "ad_groups"

    @property
    def key_properties(self):
        return ["resource_name"]

    @property
    def replication_key(self):
        return None

    @property
    def replication_method(self):
        return "FULL_TABLE"

    def gen_records(self, service, customer_id):
        query = """
            SELECT
                ad_group.ad_rotation_mode,
                ad_group.audience_setting.use_audience_grouped,
                ad_group.base_ad_group,
                ad_group.campaign,
                ad_group.cpc_bid_micros,
                ad_group.cpm_bid_micros,
                ad_group.cpv_bid_micros,
                ad_group.display_custom_bid_dimension,
                ad_group.effective_cpc_bid_micros,
                ad_group.effective_target_cpa_micros,
                ad_group.effective_target_cpa_source,
                ad_group.effective_target_roas,
                ad_group.effective_target_roas_source,
                ad_group.excluded_parent_asset_field_types,
                ad_group.final_url_suffix,
                ad_group.id,
                ad_group.labels,
                ad_group.name,
                ad_group.percent_cpc_bid_micros,
                ad_group.resource_name,
                ad_group.status,
                ad_group.target_cpa_micros,
                ad_group.target_cpm_micros,
                ad_group.target_roas,
                ad_group.targeting_setting.target_restrictions,
                ad_group.tracking_url_template,
                ad_group.type,
                ad_group.url_custom_parameters,
                customer.id
            FROM ad_group
        """

        resp = service.search_stream(customer_id=customer_id, query=query)

        for batch in resp:
            for row in batch.results:
                ag = row.ad_group
                yield {
                    "ad_rotation_mode": ag.ad_rotation_mode,
                    "use_audience_grouped": ag.audience_setting.use_audience_grouped,
                    "base_ad_group": ag.base_ad_group,
                    "campaign": ag.campaign,
                    "campaign_id": int(ag.campaign.split("/campaigns/")[1]),
                    "cpc_bid_micros": ag.cpc_bid_micros,
                    "cpm_bid_micros": ag.cpm_bid_micros,
                    "cpv_bid_micros": ag.cpv_bid_micros,
                    "display_custom_bid_dimension": ag.display_custom_bid_dimension,
                    "effective_cpc_bid_micros": ag.effective_cpc_bid_micros,
                    "effective_target_cpa_micros": ag.effective_target_cpa_micros,
                    "effective_target_cpa_source": ag.effective_target_cpa_source,
                    "effective_target_roas": ag.effective_target_roas,
                    "effective_target_roas_source": ag.effective_target_roas_source,
                    "excluded_parent_asset_field_types": ag.excluded_parent_asset_field_types,
                    "explorer_auto_optimizer_setting": None,
                    "final_url_suffix": ag.final_url_suffix,
                    "id": ag.id,
                    "labels": list(ag.labels),
                    "name": ag.name,
                    "percent_cpc_bid_micros": ag.percent_cpc_bid_micros,
                    "resource_name": ag.resource_name,
                    "status": ag.status,
                    "target_cpa_micros": ag.target_cpa_micros,
                    "target_cpm_micros": ag.target_cpm_micros,
                    "target_roas": ag.target_roas,
                    "target_restrictions": ag.targeting_setting.target_restrictions,
                    "tracking_url_template": ag.tracking_url_template,
                    "ad_group_type": ag.type_,
                    "url_custom_parameters": list(ag.url_custom_parameters),
                    "customer_id": row.customer.id,
                }

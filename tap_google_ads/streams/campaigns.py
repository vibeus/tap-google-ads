import singer
import json
import time
from singer import metadata
from datetime import datetime, timedelta
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from .base import Base

LOGGER = singer.get_logger()

class Campaigns(Base):
    @property
    def name(self):
        return "campaigns"

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
                campaign.accessible_bidding_strategy,
                campaign.ad_serving_optimization_status,
                campaign.advertising_channel_sub_type,
                campaign.advertising_channel_type,
                campaign.app_campaign_setting.app_id,
                campaign.app_campaign_setting.app_store,
                campaign.app_campaign_setting.bidding_strategy_goal_type,
                campaign.base_campaign,
                campaign.bidding_strategy,
                campaign.bidding_strategy_type,
                campaign.campaign_budget,
                campaign.commission.commission_rate_micros,
                campaign.dynamic_search_ads_setting.domain_name,
                campaign.dynamic_search_ads_setting.feeds,
                campaign.dynamic_search_ads_setting.language_code,
                campaign.dynamic_search_ads_setting.use_supplied_urls_only,
                campaign.end_date,
                campaign.excluded_parent_asset_field_types,
                campaign.experiment_type,
                campaign.final_url_suffix,
                campaign.frequency_caps,
                campaign.geo_target_type_setting.negative_geo_target_type,
                campaign.geo_target_type_setting.positive_geo_target_type,
                campaign.hotel_setting.hotel_center_id,
                campaign.id,
                campaign.labels,
                campaign.local_campaign_setting.location_source_type,
                campaign.manual_cpc.enhanced_cpc_enabled,
                campaign.manual_cpm,
                campaign.manual_cpv,
                campaign.maximize_conversion_value.target_roas,
                campaign.name,
                campaign.network_settings.target_content_network,
                campaign.network_settings.target_google_search,
                campaign.network_settings.target_partner_search_network,
                campaign.network_settings.target_search_network,
                campaign.optimization_goal_setting.optimization_goal_types,
                campaign.optimization_score,
                campaign.payment_mode,
                campaign.percent_cpc.cpc_bid_ceiling_micros,
                campaign.percent_cpc.enhanced_cpc_enabled,
                campaign.real_time_bidding_setting.opt_in,
                campaign.resource_name,
                campaign.selective_optimization.conversion_actions,
                campaign.serving_status,
                campaign.shopping_setting.campaign_priority,
                campaign.shopping_setting.enable_local,
                campaign.shopping_setting.merchant_id,
                campaign.start_date,
                campaign.status,
                campaign.target_cpa.cpc_bid_ceiling_micros,
                campaign.target_cpa.cpc_bid_floor_micros,
                campaign.target_cpa.target_cpa_micros,
                campaign.target_impression_share.cpc_bid_ceiling_micros,
                campaign.target_impression_share.location,
                campaign.target_impression_share.location_fraction_micros,
                campaign.target_roas.cpc_bid_ceiling_micros,
                campaign.target_roas.cpc_bid_floor_micros,
                campaign.target_roas.target_roas,
                campaign.target_spend.cpc_bid_ceiling_micros,
                campaign.target_spend.target_spend_micros,
                campaign.targeting_setting.target_restrictions,
                campaign.tracking_setting.tracking_url,
                campaign.tracking_url_template,
                campaign.url_custom_parameters,
                campaign.vanity_pharma.vanity_pharma_display_url_mode,
                campaign.vanity_pharma.vanity_pharma_text,
                campaign.video_brand_safety_suitability,
                customer.id
            FROM campaign
        """

        resp = service.search_stream(customer_id=customer_id, query=query)

        for batch in resp:
            for row in batch.results:
                c = row.campaign
                yield {
                    "accessible_bidding_strategy": c.accessible_bidding_strategy,
                    "ad_serving_optimization_status": c.ad_serving_optimization_status,
                    "advertising_channel_sub_type": c.advertising_channel_sub_type,
                    "advertising_channel_type": c.advertising_channel_type,
                    "app_campaign_setting": {
                        "app_id": c.app_campaign_setting.app_id,
                        "app_store": c.app_campaign_setting.app_store,
                        "bidding_strategy_goal_type": c.app_campaign_setting.bidding_strategy_goal_type,
                    },
                    "base_campaign": c.base_campaign,
                    "bidding_strategy": c.bidding_strategy,
                    "bidding_strategy_type": c.bidding_strategy_type,
                    "campaign_budget": c.campaign_budget,
                    "commission": {
                        "commission_rate_micros": c.commission.commission_rate_micros,
                    },
                    "dynamic_search_ads_setting": {
                        "domain_name": c.dynamic_search_ads_setting.domain_name,
                        "feeds": list(c.dynamic_search_ads_setting.feeds),
                        "language_code": c.dynamic_search_ads_setting.language_code,
                        "use_supplied_urls_only": c.dynamic_search_ads_setting.use_supplied_urls_only,
                    },
                    "end_date": c.end_date,
                    "excluded_parent_asset_field_types": list(c.excluded_parent_asset_field_types),
                    "experiment_type": c.experiment_type,
                    "final_url_suffix": c.final_url_suffix,
                    "frequency_caps": [
                        {
                            "key": {
                                "level": x.key.level,
                                "event_type": x.key.event_type,
                                "time_unit": x.key.time_unit,
                                "time_length": x.key.time_length,
                            },
                            "cap": x.cap,
                        }
                        for x in c.frequency_caps
                    ],
                    "geo_target_type_setting": {
                        "negative_geo_target_type": c.geo_target_type_setting.negative_geo_target_type,
                        "positive_geo_target_type": c.geo_target_type_setting.positive_geo_target_type,
                    },
                    "hotel_setting": {"hotel_center_id": c.hotel_setting.hotel_center_id},
                    "id": c.id,
                    "labels": list(c.labels),
                    "local_campaign_setting": {"location_source_type": c.local_campaign_setting.location_source_type},
                    "manual_cpc": {"enhanced_cpc_enabled": c.manual_cpc.enhanced_cpc_enabled},
                    "manual_cpm": str(c.manual_cpm),
                    "manual_cpv": str(c.manual_cpv),
                    "maximize_conversion_value": {"target_roas": c.maximize_conversion_value.target_roas},
                    "maximize_conversions": {"target_cpa": None},
                    "name": c.name,
                    "network_settings": {
                        "target_content_network": c.network_settings.target_content_network,
                        "target_google_search": c.network_settings.target_google_search,
                        "target_partner_search_network": c.network_settings.target_partner_search_network,
                        "target_search_network": c.network_settings.target_search_network,
                    },
                    "optimization_goal_setting": {
                        "optimization_goal_types": list(c.optimization_goal_setting.optimization_goal_types)
                    },
                    "optimization_score": c.optimization_score,
                    "payment_mode": c.payment_mode,
                    "percent_cpc": {
                        "cpc_bid_ceiling_micros": c.percent_cpc.cpc_bid_ceiling_micros,
                        "enhanced_cpc_enabled": c.percent_cpc.enhanced_cpc_enabled,
                    },
                    "real_time_bidding_setting": {"opt_in": c.real_time_bidding_setting.opt_in},
                    "resource_name": c.resource_name,
                    "selective_optimization": {"conversion_actions": list(c.selective_optimization.conversion_actions)},
                    "serving_status": c.serving_status,
                    "shopping_setting": {
                        "campaign_priority": c.shopping_setting.campaign_priority,
                        "enable_local": c.shopping_setting.enable_local,
                        "merchant_id": c.shopping_setting.merchant_id,
                    },
                    "start_date": c.start_date,
                    "status": c.status,
                    "target_cpa": {
                        "cpc_bid_ceiling_micros": c.target_cpa.cpc_bid_ceiling_micros,
                        "cpc_bid_floor_micros": c.target_cpa.cpc_bid_floor_micros,
                        "target_cpa_micros": c.target_cpa.target_cpa_micros,
                    },
                    "target_cpm": None,
                    "target_impression_share": {
                        "cpc_bid_ceiling_micros": c.target_impression_share.cpc_bid_ceiling_micros,
                        "location": c.target_impression_share.location,
                        "location_fraction_micros": c.target_impression_share.location_fraction_micros,
                    },
                    "target_roas": {
                        "cpc_bid_ceiling_micros": c.target_roas.cpc_bid_ceiling_micros,
                        "cpc_bid_floor_micros": c.target_roas.cpc_bid_floor_micros,
                        "target_roas": c.target_roas.target_roas,
                    },
                    "target_spend": {
                        "cpc_bid_ceiling_micros": c.target_spend.cpc_bid_ceiling_micros,
                        "target_spend_micros": c.target_spend.target_spend_micros,
                    },
                    "targeting_setting": {
                        "target_restrictions": [
                            {"targeting_dimension": x.targeting_dimension, "bid_only": x.bid_only}
                            for x in c.targeting_setting.target_restrictions
                        ]
                    },
                    "tracking_setting": {"tracking_url": c.tracking_setting.tracking_url},
                    "tracking_url_template": c.tracking_url_template,
                    "url_custom_parameters": [{"key": x.key, "value": x.value} for x in c.url_custom_parameters],
                    "vanity_pharma": {
                        "vanity_pharma_display_url_mode": c.vanity_pharma.vanity_pharma_display_url_mode,
                        "vanity_pharma_text": c.vanity_pharma.vanity_pharma_text,
                    },
                    "video_brand_safety_suitability": c.video_brand_safety_suitability,
                    "customer_id": row.customer.id,
                }

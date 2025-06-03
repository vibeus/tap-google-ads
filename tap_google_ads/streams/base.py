import singer
import json
import time
from singer import metadata
from datetime import datetime, timedelta, timezone
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

LOGGER = singer.get_logger()
CLIENT_CONFIG_KEYS = ["developer_token", "client_id", "client_secret", "refresh_token", "login_customer_id", "use_proto_plus"]
DEFAULT_BACKOFF_SECONDS = 60

class Base:
    def __init__(self):
        self._start_date = ""
        self._state = {}

    @property
    def name(self):
        return "base"

    @property
    def key_properties(self):
        return ["id"]

    @property
    def replication_key(self):
        return None

    @property
    def replication_method(self):
        return "FULL_TABLE"

    @property
    def state(self):
        return self._state

    def get_metadata(self, schema):
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=self.key_properties,
            valid_replication_keys=[],
            replication_method=self.replication_method,
        )

        return mdata

    def get_tap_data(self, config, state):
        client_config = {key: value for key, value in config.items() if key in CLIENT_CONFIG_KEYS}
        client = GoogleAdsClient.load_from_dict(config_dict=client_config, version="v18")
        service = client.get_service("GoogleAdsService")

        for customer_id in config["customer_ids"]:
            yield from self.gen_records(service, customer_id)

    def gen_records(self, service, customer_id):
        query = """
            SELECT * FROM fields
        """

        resp = service.search_stream(customer_id=customer_id, query=query)

        for batch in resp:
            for row in batch.results:
                yield {"customer_id": row.customer.id,}


class Incremental(Base):
    @property
    def replication_method(self):
        return "INCREMENTAL"

    def get_tap_data(self, config, state):
      today = datetime.now(timezone.utc).date().isoformat()
      self._start_date = config.get("start_date", today)
      self._backoff_seconds = config.get("rate_limit_backoff_seconds", DEFAULT_BACKOFF_SECONDS)
      self._state = state.copy()

      client_config = {key: value for key, value in config.items() if key in CLIENT_CONFIG_KEYS}
      client = GoogleAdsClient.load_from_dict(config_dict=client_config, version="v18")
      service = client.get_service("GoogleAdsService")

      for customer_id in config["customer_ids"]:
          yield from self.gen_records(config, service, customer_id)
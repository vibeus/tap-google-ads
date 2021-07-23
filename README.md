# tap-google-ads

This is a [Singer][1] tap that produces JSON-formatted data following the [Singer spec][2].

This tap:

- Pulls raw data from [Google Ads API v8][3]
- Extracts the following resources:
  - [Campaign][4] with selected segments and metrics that we care about
- Outputs the schema for each resource
- Incrementally pulls metrics data based on the input state

## Install

```
pip install git+https://github.com/vibeus/tap-google-ads.git@v0.1.0
```

## Usage

1. Follow [Singer.io Best Practices][5] for setting up separate `tap` and `target` virtualenvs to avoid version conflicts.
2. Create a [config file][6] ~/config.json with [Google Ads API Credentials][7].
    ```json
    {
      "developer_token": "env[DEVELOPER_TOKEN]",
      "client_id": "env[GOOGLE_CLIENT_ID]",
      "client_secret": "env[GOOGLE_CLIENT_SECRET]",
      "refresh_token": "env[GOOGLE_OAUTH_REFRESH_TOKEN]",
      "login_customer_id": "1234567890",
      "customer_ids": ["1234567890"],
      "start_date": "2021-07-01T00:00:00Z"
    }
    ```
3. Discover catalog: `tap-google-ads -c config.json -d > catalog.json`
4. Select `campaigns` and `campaign_metrics` stream in the generated `catalog.json`.
    ```
    ...
    "stream": "campaigns",
    "metadata": [
      {
        "breadcrumb": [],
        "metadata": {
          "table-key-properties": [
            "resource_name"
          ],
          "forced-replication-method": "FULL_TABLE",
          "valid-replication-keys": [],
          "inclusion": "available",
          "selected": true  <-- Somewhere in the huge catalog file, in stream metadata.
        }
      },
      ...
    ]
    ...
    "stream": "campaign_metrics",
    "metadata": [
      {
        "breadcrumb": [],
        "metadata": {
          "table-key-properties": [
            "campaign_id",
            "ad_network_type",
            "date",
            "device"
          ],
          "forced-replication-method": "INCREMENTAL",
          "valid-replication-keys": [
            "date"
          ],
          "inclusion": "available",
          "selected": true  <-- Somewhere in the huge catalog file, in stream metadata.
        }
      },
      ...
    ]
    ...
    ```
5. Use following command to sync all campaigns and their metrics.
```bash
tap-google-ads -c config.json --catalog catalog.json > output.txt
```

---

Copyright &copy; 2021 Vibe Inc

[1]: https://singer.io
[2]: https://github.com/singer-io/getting-started/blob/master/SPEC.md
[3]: https://developers.google.com/google-ads/api/reference/rpc/v8/overview
[4]: https://developers.google.com/google-ads/api/fields/v8/campaign
[5]: https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target
[6]: https://github.com/vibeus/tap-google-ads/blob/master/sample_config.json
[7]: https://developers.google.com/google-ads/api/docs/client-libs/python/

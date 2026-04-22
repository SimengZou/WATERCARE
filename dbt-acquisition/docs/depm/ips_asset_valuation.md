# ips_asset_valuation

Generated from anySQL Connector dEPM_IPS_ASSET_VALUATION

## Overview

| Property | Value |
|---|---|
| **Source system** | `depm` |
| **Source table** | `depm_ips_asset_valuation` |
| **Materialization** | `incremental` |
| **Primary keys** | `assetkey`, `assetbookid` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `assetrecordtype` | `string` | `varchar` |  | `not_null` | (required) Max length: 10 |
| `assetkey` | `string` | `varchar` | `PK` | `not_null` | (required) Max length: 100 |
| `assetbookid` | `string` | `varchar` | `PK` | `not_null` | (required) Max length: 255 |
| `assetdescription` | `string` | `varchar` |  | `not_null` | (required) Max length: 255 |
| `assetclass` | `string` | `varchar` |  | `not_null` | (required) Max length: 255 |
| `assetcostcentre` | `string` | `varchar` |  | `not_null` | (required) Max length: 255 |
| `assetaquisitiontype` | `string` | `varchar` |  | `not_null` | (required) Max length: 255 |
| `assetaquisitiondate` | `timestamp` | `timestamp_ntz` |  | `not_null` | (required) Max length: 24 |
| `assetdepreciationmethod` | `integer` | `int` |  | `not_null` | (required) |
| `assetexpectedlife` | `float` | `float` |  | `not_null` | (required) |
| `assetinitialcost` | `float` | `float` |  | `not_null` | (required) |
| `comments` | `string` | `varchar` |  |  | Max length: 255 |
| `ionstatus` | `integer` | `int` |  | `not_null` | (required) |
| `timestamp` | `timestamp` | `timestamp_ntz` |  |  | Max length: 24 |
| `project` | `string` | `varchar` |  |  | Max length: 255 |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# ttdpm530

LN ttdpm530 IMS Parameters table, Generated 2026-04-10T19:42:40Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_ttdpm530` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `sequ` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `sequ` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `burl` | `string` | `varchar` |  |  | Base URL. Max length: 999 |
| `oack` | `string` | `varchar` |  |  | Base OAuth Consumer Key. Max length: 999 |
| `oask` | `string` | `varchar` |  |  | Base OAuth Secret Key. Max length: 999 |
| `loid` | `string` | `varchar` |  |  | Logical ID. Max length: 250 |
| `llid` | `string` | `varchar` |  |  | Local Logical ID. Max length: 250 |
| `rurl` | `string` | `varchar` |  |  | Data Catalog URL. Max length: 999 |
| `rack` | `string` | `varchar` |  |  | Data Catalog OAuth Consumer Key. Max length: 999 |
| `rask` | `string` | `varchar` |  |  | Data Catalog OAuth Secret Key. Max length: 999 |
| `pmil` | `integer` | `int` |  |  | Publish Method Initial Load. Allowed values: 5, 10, 15 |
| `pmil_kw` | `string` | `varchar` |  |  | Publish Method Initial Load (keyword). Allowed values: ttdpm.pmth.batch, ttdpm.pmth.streaming, ttdpm.pmth.ims |
| `pmch` | `integer` | `int` |  |  | Publish Method Changes. Allowed values: 5, 10, 15 |
| `pmch_kw` | `string` | `varchar` |  |  | Publish Method Changes (keyword). Allowed values: ttdpm.pmth.batch, ttdpm.pmth.streaming, ttdpm.pmth.ims |
| `deleted` | `boolean` | `boolean` |  | `not_null` | (required) Whether record is deleted |
| `username` | `string` | `varchar` |  | `not_null` | (required) User responsible for record action. Max length: 8 |
| `timestamp` | `timestamp` | `timestamp_ntz` |  | `not_null` | (required) Time the action occurred |
| `sequencenumber` | `integer` | `int` |  | `not_null` | (required) Sequence number of the action |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

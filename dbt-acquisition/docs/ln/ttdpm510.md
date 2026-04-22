# ttdpm510

LN ttdpm510 Data Set Companies table, Generated 2026-04-10T19:42:40Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_ttdpm510` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `pacc`, `dset`, `comp` |
| **Column count** | 19 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `pacc` | `string` | `varchar` | `PK` | `not_null` | (required) Package Combination. Max length: 8 |
| `dset` | `string` | `varchar` | `PK` | `not_null` | (required) Data Set. Max length: 30 |
| `comp` | `integer` | `int` | `PK` | `not_null` | (required) Company |
| `pubd` | `integer` | `int` |  |  | Publish Data. Allowed values: 1, 2 |
| `pubd_kw` | `string` | `varchar` |  |  | Publish Data (keyword). Allowed values: ttyeno.yes, ttyeno.no |
| `pacc_dset_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ttdpm505 Data Sets to Publish |
| `pacc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ttaad120 Package Combinations |
| `dset_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ttdpm500 Data Sets |
| `comp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ttaad100 Companies |
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

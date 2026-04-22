# tcemm135

LN tcemm135 Clusters table, Generated 2026-04-10T19:41:06Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcemm135` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `clus` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `clus` | `string` | `varchar` | `PK` | `not_null` | (required) Cluster. Max length: 3 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `extl` | `integer` | `int` |  |  | External Cluster. Allowed values: 1, 2 |
| `extl_kw` | `string` | `varchar` |  |  | External Cluster (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hsub` | `integer` | `int` |  |  | Holds Subcontractor Sites. Allowed values: 1, 2 |
| `hsub_kw` | `string` | `varchar` |  |  | Holds Subcontractor Sites (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cust` | `integer` | `int` |  |  | Customer Site. Allowed values: 1, 2 |
| `cust_kw` | `string` | `varchar` |  |  | Customer Site (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `expl` | `integer` | `int` |  |  | Exclude From Enterprise Planning. Allowed values: 1, 2 |
| `expl_kw` | `string` | `varchar` |  |  | Exclude From Enterprise Planning (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
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

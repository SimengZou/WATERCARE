# r5conparts

eam_R5CONPARTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5conparts` |
| **Materialization** | `incremental` |
| **Primary keys** | `cpa_contract`, `cpa_part`, `cpa_part_org` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `cpa_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CPA_LASTSAVED |
| `cpa_supplier` | `string` | `varchar` |  |  | CPA_SUPPLIER |
| `cpa_part` | `string` | `varchar` | `PK` |  | CPA_PART |
| `cpa_multiply` | `float` | `float` |  |  | CPA_MULTIPLY |
| `cpa_leadtime` | `float` | `float` |  |  | CPA_LEADTIME |
| `cpa_ref` | `string` | `varchar` |  |  | CPA_REF |
| `cpa_puruom` | `string` | `varchar` |  |  | CPA_PURUOM |
| `cpa_supplier_org` | `string` | `varchar` |  |  | CPA_SUPPLIER_ORG |
| `cpa_part_org` | `string` | `varchar` | `PK` |  | CPA_PART_ORG |
| `cpa_updatecount` | `float` | `float` |  |  | CPA_UPDATECOUNT |
| `cpa_contract` | `string` | `varchar` | `PK` |  | CPA_CONTRACT |
| `cpa_price` | `float` | `float` |  |  | CPA_PRICE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

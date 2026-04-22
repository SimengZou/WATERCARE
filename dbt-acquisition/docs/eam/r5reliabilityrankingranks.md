# r5reliabilityrankingranks

eam_R5RELIABILITYRANKINGRANKS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5reliabilityrankingranks` |
| **Materialization** | `incremental` |
| **Primary keys** | `rrr_pk` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rrr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | RRR_LASTSAVED |
| `rrr_reliabilityranking` | `string` | `varchar` |  |  | RRR_RELIABILITYRANKING |
| `rrr_org` | `string` | `varchar` |  |  | RRR_ORG |
| `rrr_revision` | `float` | `float` |  |  | RRR_REVISION |
| `rrr_minvalue` | `float` | `float` |  |  | RRR_MINVALUE |
| `rrr_maxvalue` | `float` | `float` |  |  | RRR_MAXVALUE |
| `rrr_image` | `string` | `varchar` |  |  | RRR_IMAGE |
| `rrr_updatecount` | `float` | `float` |  |  | RRR_UPDATECOUNT |
| `rrr_criticality` | `string` | `varchar` |  |  | RRR_CRITICALITY |
| `rrr_color` | `string` | `varchar` |  |  | RRR_COLOR |
| `rrr_icon` | `string` | `varchar` |  |  | RRR_ICON |
| `rrr_iconpath` | `string` | `varchar` |  |  | RRR_ICONPATH |
| `rrr_pk` | `string` | `varchar` | `PK` | `unique` | RRR_PK |
| `rrr_rrindex` | `string` | `varchar` |  |  | RRR_RRINDEX |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

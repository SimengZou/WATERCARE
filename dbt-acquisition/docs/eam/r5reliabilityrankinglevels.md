# r5reliabilityrankinglevels

eam_R5RELIABILITYRANKINGLEVELS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5reliabilityrankinglevels` |
| **Materialization** | `incremental` |
| **Primary keys** | `rrl_pk` |
| **Column count** | 28 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rrl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | RRL_LASTSAVED |
| `rrl_allowoperatorchecklist` | `string` | `varchar` |  |  | RRL_ALLOWOPERATORCHECKLIST |
| `rrl_org` | `string` | `varchar` |  |  | RRL_ORG |
| `rrl_revision` | `float` | `float` |  |  | RRL_REVISION |
| `rrl_parent` | `string` | `varchar` |  |  | RRL_PARENT |
| `rrl_code` | `string` | `varchar` |  |  | RRL_CODE |
| `rrl_desc` | `string` | `varchar` |  |  | RRL_DESC |
| `rrl_questionlevel` | `string` | `varchar` |  |  | RRL_QUESTIONLEVEL |
| `rrl_question` | `string` | `varchar` |  |  | RRL_QUESTION |
| `rrl_seq` | `float` | `float` |  |  | RRL_SEQ |
| `rrl_formula` | `string` | `varchar` |  |  | RRL_FORMULA |
| `rrl_updatecount` | `float` | `float` |  |  | RRL_UPDATECOUNT |
| `rrl_numeric` | `string` | `varchar` |  |  | RRL_NUMERIC |
| `rrl_integer` | `string` | `varchar` |  |  | RRL_INTEGER |
| `rrl_min` | `float` | `float` |  |  | RRL_MIN |
| `rrl_max` | `float` | `float` |  |  | RRL_MAX |
| `rrl_calculated` | `string` | `varchar` |  |  | RRL_CALCULATED |
| `rrl_querycode` | `string` | `varchar` |  |  | RRL_QUERYCODE |
| `rrl_aspect` | `string` | `varchar` |  |  | RRL_ASPECT |
| `rrl_checklisttype` | `string` | `varchar` |  |  | RRL_CHECKLISTTYPE |
| `rrl_pk` | `string` | `varchar` | `PK` | `unique` | RRL_PK |
| `rrl_reliabilityranking` | `string` | `varchar` |  |  | RRL_RELIABILITYRANKING |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5pmpsessionlines

eam_R5PMPSESSIONLINES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5pmpsessionlines` |
| **Materialization** | `incremental` |
| **Primary keys** | `ppl_line` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ppl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PPL_LASTSAVED |
| `ppl_object` | `string` | `varchar` |  |  | PPL_OBJECT |
| `ppl_objorg` | `string` | `varchar` |  |  | PPL_OBJORG |
| `ppl_ppm` | `string` | `varchar` |  |  | PPL_PPM |
| `ppl_ppopk` | `float` | `float` |  |  | PPL_PPOPK |
| `ppl_sessionid` | `float` | `float` |  |  | PPL_SESSIONID |
| `ppl_updatecount` | `float` | `float` |  |  | PPL_UPDATECOUNT |
| `ppl_line` | `float` | `float` | `PK` | `unique` | PPL_LINE |
| `ppl_nestingref` | `string` | `varchar` |  |  | PPL_NESTINGREF |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

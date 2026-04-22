# r5acdparams

eam_R5ACDPARAMS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5acdparams` |
| **Materialization** | `incremental` |
| **Primary keys** | `adp_segment` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `adp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ADP_LASTSAVED |
| `adp_datatype` | `string` | `varchar` |  |  | ADP_DATATYPE |
| `adp_prompt` | `string` | `varchar` |  |  | ADP_PROMPT |
| `adp_promptx` | `float` | `float` |  |  | ADP_PROMPTX |
| `adp_prompty` | `float` | `float` |  |  | ADP_PROMPTY |
| `adp_segmentx` | `float` | `float` |  |  | ADP_SEGMENTX |
| `adp_segmenty` | `float` | `float` |  |  | ADP_SEGMENTY |
| `adp_seq` | `float` | `float` |  |  | ADP_SEQ |
| `adp_required` | `string` | `varchar` |  |  | ADP_REQUIRED |
| `adp_default` | `string` | `varchar` |  |  | ADP_DEFAULT |
| `adp_hint` | `string` | `varchar` |  |  | ADP_HINT |
| `adp_lovsql` | `string` | `varchar` |  |  | ADP_LOVSQL |
| `adp_descsql` | `string` | `varchar` |  |  | ADP_DESCSQL |
| `adp_updatecount` | `float` | `float` |  |  | ADP_UPDATECOUNT |
| `adp_query` | `string` | `varchar` |  |  | ADP_QUERY |
| `adp_valuelookup` | `string` | `varchar` |  |  | ADP_VALUELOOKUP |
| `adp_segment` | `string` | `varchar` | `PK` | `unique` | ADP_SEGMENT |
| `adp_length` | `float` | `float` |  |  | ADP_LENGTH |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

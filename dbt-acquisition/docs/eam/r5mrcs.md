# r5mrcs

eam_R5MRCS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mrcs` |
| **Materialization** | `incremental` |
| **Primary keys** | `mrc_code` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `mrc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MRC_LASTSAVED |
| `mrc_availableforcus` | `string` | `varchar` |  |  | MRC_AVAILABLEFORCUS |
| `mrc_class` | `string` | `varchar` |  |  | MRC_CLASS |
| `mrc_store` | `string` | `varchar` |  |  | MRC_STORE |
| `mrc_dfltscreener` | `string` | `varchar` |  |  | MRC_DFLTSCREENER |
| `mrc_schedgroup` | `string` | `varchar` |  |  | MRC_SCHEDGROUP |
| `mrc_capacity` | `float` | `float` |  |  | MRC_CAPACITY |
| `mrc_org` | `string` | `varchar` |  |  | MRC_ORG |
| `mrc_class_org` | `string` | `varchar` |  |  | MRC_CLASS_ORG |
| `mrc_updatecount` | `float` | `float` |  |  | MRC_UPDATECOUNT |
| `mrc_created` | `timestamp` | `timestamp_ntz` |  |  | MRC_CREATED |
| `mrc_updated` | `timestamp` | `timestamp_ntz` |  |  | MRC_UPDATED |
| `mrc_notused` | `string` | `varchar` |  |  | MRC_NOTUSED |
| `mrc_segmentvalue` | `string` | `varchar` |  |  | MRC_SEGMENTVALUE |
| `mrc_depot` | `string` | `varchar` |  |  | MRC_DEPOT |
| `mrc_depot_org` | `string` | `varchar` |  |  | MRC_DEPOT_ORG |
| `mrc_code` | `string` | `varchar` | `PK` | `unique` | MRC_CODE |
| `mrc_desc` | `string` | `varchar` |  |  | MRC_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

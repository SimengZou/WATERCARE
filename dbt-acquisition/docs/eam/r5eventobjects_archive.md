# r5eventobjects_archive

eam_R5EVENTOBJECTS_ARCHIVE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5eventobjects_archive` |
| **Materialization** | `incremental` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `aeo_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | AEO_LASTSAVED |
| `aeo_event` | `string` | `varchar` |  |  | AEO_EVENT |
| `aeo_obtype` | `string` | `varchar` |  |  | AEO_OBTYPE |
| `aeo_obrtype` | `string` | `varchar` |  |  | AEO_OBRTYPE |
| `aeo_object` | `string` | `varchar` |  |  | AEO_OBJECT |
| `aeo_level` | `float` | `float` |  |  | AEO_LEVEL |
| `aeo_downtime` | `string` | `varchar` |  |  | AEO_DOWNTIME |
| `aeo_object_org` | `string` | `varchar` |  |  | AEO_OBJECT_ORG |
| `aeo_updatecount` | `float` | `float` |  |  | AEO_UPDATECOUNT |
| `aeo_frompoint` | `float` | `float` |  |  | AEO_FROMPOINT |
| `aeo_topoint` | `float` | `float` |  |  | AEO_TOPOINT |
| `aeo_weightpercentage` | `float` | `float` |  |  | AEO_WEIGHTPERCENTAGE |
| `aeo_archive` | `float` | `float` |  |  | AEO_ARCHIVE |
| `aeo_rollup` | `string` | `varchar` |  |  | AEO_ROLLUP |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

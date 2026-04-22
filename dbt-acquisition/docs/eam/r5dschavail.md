# r5dschavail

eam_R5DSCHAVAIL

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dschavail` |
| **Materialization** | `incremental` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `sha_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | SHA_LASTSAVED |
| `sha_person` | `string` | `varchar` |  |  | SHA_PERSON |
| `sha_shift` | `string` | `varchar` |  |  | SHA_SHIFT |
| `sha_hours` | `float` | `float` |  |  | SHA_HOURS |
| `sha_mrc` | `string` | `varchar` |  |  | SHA_MRC |
| `sha_gendate` | `timestamp` | `timestamp_ntz` |  |  | SHA_GENDATE |
| `sha_updatecount` | `float` | `float` |  |  | SHA_UPDATECOUNT |
| `sha_date` | `timestamp` | `timestamp_ntz` |  |  | SHA_DATE |
| `sha_trade` | `string` | `varchar` |  |  | SHA_TRADE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

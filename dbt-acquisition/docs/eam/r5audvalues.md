# r5audvalues

eam_R5AUDVALUES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5audvalues` |
| **Materialization** | `incremental` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ava_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | AVA_LASTSAVED |
| `ava_primaryid` | `string` | `varchar` |  |  | AVA_PRIMARYID |
| `ava_secondaryid` | `string` | `varchar` |  |  | AVA_SECONDARYID |
| `ava_from` | `string` | `varchar` |  |  | AVA_FROM |
| `ava_to` | `string` | `varchar` |  |  | AVA_TO |
| `ava_changed` | `timestamp` | `timestamp_ntz` |  |  | AVA_CHANGED |
| `ava_modifiedby` | `string` | `varchar` |  |  | AVA_MODIFIEDBY |
| `ava_function` | `string` | `varchar` |  |  | AVA_FUNCTION |
| `ava_updated` | `string` | `varchar` |  |  | AVA_UPDATED |
| `ava_inserted` | `string` | `varchar` |  |  | AVA_INSERTED |
| `ava_deleted` | `string` | `varchar` |  |  | AVA_DELETED |
| `ava_updatecount` | `float` | `float` |  |  | AVA_UPDATECOUNT |
| `ava_scode` | `string` | `varchar` |  |  | AVA_SCODE |
| `ava_mobile` | `string` | `varchar` |  |  | AVA_MOBILE |
| `ava_primaryid2` | `string` | `varchar` |  |  | AVA_PRIMARYID2 |
| `ava_attribute` | `float` | `float` |  |  | AVA_ATTRIBUTE |
| `ava_table` | `string` | `varchar` |  |  | AVA_TABLE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

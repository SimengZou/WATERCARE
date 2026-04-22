# r5availability

eam_R5AVAILABILITY

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5availability` |
| **Materialization** | `incremental` |
| **Primary keys** | `avl_date`, `avl_mrc`, `avl_trade` |
| **Column count** | 16 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `avl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | AVL_LASTSAVED |
| `avl_nethours` | `float` | `float` |  |  | AVL_NETHOURS |
| `avl_hirhours` | `float` | `float` |  |  | AVL_HIRHOURS |
| `avl_nethired` | `float` | `float` |  |  | AVL_NETHIRED |
| `avl_date` | `timestamp` | `timestamp_ntz` | `PK` |  | AVL_DATE |
| `avl_mrc` | `string` | `varchar` | `PK` |  | AVL_MRC |
| `avl_trade` | `string` | `varchar` | `PK` |  | AVL_TRADE |
| `avl_updatecount` | `float` | `float` |  |  | AVL_UPDATECOUNT |
| `avl_ownhours` | `float` | `float` |  |  | AVL_OWNHOURS |
| `avl_special` | `string` | `varchar` |  |  | AVL_SPECIAL |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

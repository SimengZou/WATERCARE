# r5mobilelistentities

eam_R5MOBILELISTENTITIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mobilelistentities` |
| **Materialization** | `incremental` |
| **Column count** | 26 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `let_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LET_LASTSAVED |
| `let_mask` | `float` | `float` |  |  | LET_MASK |
| `let_groupid` | `float` | `float` |  |  | LET_GROUPID |
| `let_blocksize` | `float` | `float` |  |  | LET_BLOCKSIZE |
| `let_delinsupd` | `float` | `float` |  |  | LET_DELINSUPD |
| `let_filter` | `string` | `varchar` |  |  | LET_FILTER |
| `let_tablename` | `string` | `varchar` |  |  | LET_TABLENAME |
| `let_downcat` | `string` | `varchar` |  |  | LET_DOWNCAT |
| `let_orgkey` | `string` | `varchar` |  |  | LET_ORGKEY |
| `let_downgroup` | `float` | `float` |  |  | LET_DOWNGROUP |
| `let_updatecount` | `float` | `float` |  |  | LET_UPDATECOUNT |
| `let_updated` | `timestamp` | `timestamp_ntz` |  |  | LET_UPDATED |
| `let_tableupdated` | `timestamp` | `timestamp_ntz` |  |  | LET_TABLEUPDATED |
| `let_filterparams` | `string` | `varchar` |  |  | LET_FILTERPARAMS |
| `let_mastertable` | `string` | `varchar` |  |  | LET_MASTERTABLE |
| `let_cachedata` | `string` | `varchar` |  |  | LET_CACHEDATA |
| `let_rentities` | `string` | `varchar` |  |  | LET_RENTITIES |
| `let_ddspyid` | `float` | `float` |  |  | LET_DDSPYID |
| `let_datagrid` | `string` | `varchar` |  |  | LET_DATAGRID |
| `let_seqno` | `float` | `float` |  |  | LET_SEQNO |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

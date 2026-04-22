# r5usertabcolumns

eam_R5USERTABCOLUMNS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5usertabcolumns` |
| **Materialization** | `incremental` |
| **Column count** | 15 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `utc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | UTC_LASTSAVED |
| `utc_columnname` | `string` | `varchar` |  |  | UTC_COLUMNNAME |
| `utc_datatype` | `string` | `varchar` |  |  | UTC_DATATYPE |
| `utc_obj_xtype` | `string` | `varchar` |  |  | UTC_OBJ_XTYPE |
| `utc_collation` | `string` | `varchar` |  |  | UTC_COLLATION |
| `utc_isid` | `string` | `varchar` |  |  | UTC_ISID |
| `utc_database` | `string` | `varchar` |  |  | UTC_DATABASE |
| `utc_tablename` | `string` | `varchar` |  |  | UTC_TABLENAME |
| `utc_col_xtype` | `float` | `float` |  |  | UTC_COL_XTYPE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

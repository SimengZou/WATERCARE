# r5dwetldatamarts

eam_R5DWETLDATAMARTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dwetldatamarts` |
| **Materialization** | `incremental` |
| **Column count** | 11 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `etl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ETL_LASTSAVED |
| `etl_dmttable` | `string` | `varchar` |  |  | ETL_DMTTABLE |
| `etl_comdim` | `string` | `varchar` |  |  | ETL_COMDIM |
| `etl_dimtable` | `string` | `varchar` |  |  | ETL_DIMTABLE |
| `etl_module` | `string` | `varchar` |  |  | ETL_MODULE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

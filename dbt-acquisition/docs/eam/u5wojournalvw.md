# u5wojournalvw

eam_U5WOJOURNALVW

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5wojournalvw` |
| **Materialization** | `incremental` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `jrn_costtype` | `string` | `varchar` |  |  | JRN_COSTTYPE |
| `jrn_trnflag` | `string` | `varchar` |  |  | JRN_TRNFLAG |
| `jrn_debitcredit` | `string` | `varchar` |  |  | JRN_DEBITCREDIT |
| `jrn_costs` | `float` | `float` |  |  | JRN_COSTS |
| `jrn_costcentre` | `string` | `varchar` |  |  | JRN_COSTCENTRE |
| `jrn_dimension4` | `string` | `varchar` |  |  | JRN_DIMENSION4 |
| `jrn_identifier` | `string` | `varchar` |  |  | JRN_IDENTIFIER |
| `jrn_projectnumber` | `string` | `varchar` |  |  | JRN_PROJECTNUMBER |
| `jrn_projectativity` | `string` | `varchar` |  |  | JRN_PROJECTATIVITY |
| `fic_code` | `string` | `varchar` |  |  | FIC_CODE |
| `jrn_hours` | `float` | `float` |  |  | JRN_HOURS |
| `jrn_workorder` | `string` | `varchar` |  |  | JRN_WORKORDER |
| `jrn_dimension3` | `string` | `varchar` |  |  | JRN_DIMENSION3 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

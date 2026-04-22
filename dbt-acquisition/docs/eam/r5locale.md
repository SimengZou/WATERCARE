# r5locale

eam_R5LOCALE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5locale` |
| **Materialization** | `incremental` |
| **Primary keys** | `loc_code` |
| **Column count** | 22 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `loc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LOC_LASTSAVED |
| `loc_decsym` | `string` | `varchar` |  |  | LOC_DECSYM |
| `loc_mondecsym` | `string` | `varchar` |  |  | LOC_MONDECSYM |
| `loc_grpsym` | `string` | `varchar` |  |  | LOC_GRPSYM |
| `loc_grpdigits` | `float` | `float` |  |  | LOC_GRPDIGITS |
| `loc_fractdigits` | `float` | `float` |  |  | LOC_FRACTDIGITS |
| `loc_monfract` | `float` | `float` |  |  | LOC_MONFRACT |
| `loc_negsym` | `string` | `varchar` |  |  | LOC_NEGSYM |
| `loc_possym` | `string` | `varchar` |  |  | LOC_POSSYM |
| `loc_datefmt` | `string` | `varchar` |  |  | LOC_DATEFMT |
| `loc_startday` | `float` | `float` |  |  | LOC_STARTDAY |
| `loc_updatecount` | `float` | `float` |  |  | LOC_UPDATECOUNT |
| `loc_montgrpseparator` | `string` | `varchar` |  |  | LOC_MONTGRPSEPARATOR |
| `loc_montgrpdigits` | `float` | `float` |  |  | LOC_MONTGRPDIGITS |
| `loc_code` | `string` | `varchar` | `PK` | `unique` | LOC_CODE |
| `loc_desc` | `string` | `varchar` |  |  | LOC_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5userdefinedfieldsetup

eam_R5USERDEFINEDFIELDSETUP

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5userdefinedfieldsetup` |
| **Materialization** | `incremental` |
| **Primary keys** | `udf_rentity`, `udf_field` |
| **Column count** | 22 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `udf_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | UDF_LASTSAVED |
| `udf_min` | `string` | `varchar` |  |  | UDF_MIN |
| `udf_max` | `string` | `varchar` |  |  | UDF_MAX |
| `udf_print` | `string` | `varchar` |  |  | UDF_PRINT |
| `udf_uom` | `string` | `varchar` |  |  | UDF_UOM |
| `udf_lookuptype` | `string` | `varchar` |  |  | UDF_LOOKUPTYPE |
| `udf_lookupvalidate` | `string` | `varchar` |  |  | UDF_LOOKUPVALIDATE |
| `udf_lookuprentity` | `string` | `varchar` |  |  | UDF_LOOKUPRENTITY |
| `udf_datetype` | `string` | `varchar` |  |  | UDF_DATETYPE |
| `udf_numbertype` | `string` | `varchar` |  |  | UDF_NUMBERTYPE |
| `udf_curr` | `string` | `varchar` |  |  | UDF_CURR |
| `udf_enableforaddons` | `string` | `varchar` |  |  | UDF_ENABLEFORADDONS |
| `udf_updated` | `timestamp` | `timestamp_ntz` |  |  | UDF_UPDATED |
| `udf_updatecount` | `float` | `float` |  |  | UDF_UPDATECOUNT |
| `udf_rentity` | `string` | `varchar` | `PK` |  | UDF_RENTITY |
| `udf_field` | `string` | `varchar` | `PK` |  | UDF_FIELD |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

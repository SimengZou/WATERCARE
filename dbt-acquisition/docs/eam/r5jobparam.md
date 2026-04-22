# r5jobparam

eam_R5JOBPARAM

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5jobparam` |
| **Materialization** | `incremental` |
| **Column count** | 27 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `jpr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | JPR_LASTSAVED |
| `jpr_parameter` | `string` | `varchar` |  |  | JPR_PARAMETER |
| `jpr_rentity` | `string` | `varchar` |  |  | JPR_RENTITY |
| `jpr_rtype` | `string` | `varchar` |  |  | JPR_RTYPE |
| `jpr_datatype` | `string` | `varchar` |  |  | JPR_DATATYPE |
| `jpr_length` | `float` | `float` |  |  | JPR_LENGTH |
| `jpr_format` | `string` | `varchar` |  |  | JPR_FORMAT |
| `jpr_default` | `string` | `varchar` |  |  | JPR_DEFAULT |
| `jpr_fixed` | `string` | `varchar` |  |  | JPR_FIXED |
| `jpr_mandatory` | `string` | `varchar` |  |  | JPR_MANDATORY |
| `jpr_upper` | `string` | `varchar` |  |  | JPR_UPPER |
| `jpr_options` | `float` | `float` |  |  | JPR_OPTIONS |
| `jpr_remember` | `string` | `varchar` |  |  | JPR_REMEMBER |
| `jpr_test` | `string` | `varchar` |  |  | JPR_TEST |
| `jpr_query` | `string` | `varchar` |  |  | JPR_QUERY |
| `jpr_lovfunction` | `string` | `varchar` |  |  | JPR_LOVFUNCTION |
| `jpr_property` | `string` | `varchar` |  |  | JPR_PROPERTY |
| `jpr_ewslovdef` | `string` | `varchar` |  |  | JPR_EWSLOVDEF |
| `jpr_updatecount` | `float` | `float` |  |  | JPR_UPDATECOUNT |
| `jpr_name` | `string` | `varchar` |  |  | JPR_NAME |
| `jpr_line` | `float` | `float` |  |  | JPR_LINE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

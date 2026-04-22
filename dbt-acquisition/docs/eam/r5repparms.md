# r5repparms

eam_R5REPPARMS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5repparms` |
| **Materialization** | `incremental` |
| **Primary keys** | `pmt_function`, `pmt_parameter` |
| **Column count** | 30 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `pmt_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PMT_LASTSAVED |
| `pmt_parameter` | `string` | `varchar` | `PK` |  | PMT_PARAMETER |
| `pmt_rentity` | `string` | `varchar` |  |  | PMT_RENTITY |
| `pmt_rtype` | `string` | `varchar` |  |  | PMT_RTYPE |
| `pmt_datatype` | `string` | `varchar` |  |  | PMT_DATATYPE |
| `pmt_length` | `float` | `float` |  |  | PMT_LENGTH |
| `pmt_format` | `string` | `varchar` |  |  | PMT_FORMAT |
| `pmt_default` | `string` | `varchar` |  |  | PMT_DEFAULT |
| `pmt_fixed` | `string` | `varchar` |  |  | PMT_FIXED |
| `pmt_mandatory` | `string` | `varchar` |  |  | PMT_MANDATORY |
| `pmt_upper` | `string` | `varchar` |  |  | PMT_UPPER |
| `pmt_options` | `float` | `float` |  |  | PMT_OPTIONS |
| `pmt_remember` | `string` | `varchar` |  |  | PMT_REMEMBER |
| `pmt_test` | `string` | `varchar` |  |  | PMT_TEST |
| `pmt_query` | `string` | `varchar` |  |  | PMT_QUERY |
| `pmt_lovfunction` | `string` | `varchar` |  |  | PMT_LOVFUNCTION |
| `pmt_property` | `string` | `varchar` |  |  | PMT_PROPERTY |
| `pmt_updatecount` | `float` | `float` |  |  | PMT_UPDATECOUNT |
| `pmt_ewslovdef` | `string` | `varchar` |  |  | PMT_EWSLOVDEF |
| `pmt_bottext` | `string` | `varchar` |  |  | PMT_BOTTEXT |
| `pmt_updated` | `timestamp` | `timestamp_ntz` |  |  | PMT_UPDATED |
| `pmt_mekey` | `string` | `varchar` |  |  | PMT_MEKEY |
| `pmt_function` | `string` | `varchar` | `PK` |  | PMT_FUNCTION |
| `pmt_line` | `float` | `float` |  |  | PMT_LINE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

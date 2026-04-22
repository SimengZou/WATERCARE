# p100_temp

Generated from anySQL Connector dEPM_P100_Temp

## Overview

| Property | Value |
|---|---|
| **Source system** | `depm` |
| **Source table** | `depm_p100_temp` |
| **Materialization** | `incremental` |
| **Primary keys** | `version`, `period`, `project`, `wbs` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `version` | `string` | `varchar` | `PK` |  | From table dEPM_P100_Temp and column Version. Max length: 100 |
| `period` | `string` | `varchar` | `PK` |  | From table dEPM_P100_Temp and column Period. Max length: 100 |
| `project` | `string` | `varchar` | `PK` |  | From table dEPM_P100_Temp and column Project. Max length: 100 |
| `wbs` | `string` | `varchar` | `PK` |  | From table dEPM_P100_Temp and column WBS. Max length: 100 |
| `pm100` | `string` | `varchar` |  |  | From table dEPM_P100_Temp and column PM100. Max length: 100 |
| `value` | `float` | `float` |  |  | From table dEPM_P100_Temp and column Value |
| `timestamp` | `timestamp` | `timestamp_ntz` |  | `not_null` | (required) From table dEPM_P100_Temp and column Timestamp. Max length: 24 |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

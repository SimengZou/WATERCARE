# r100_temp

Generated from anySQL Connector dEPM_R100_Temp

## Overview

| Property | Value |
|---|---|
| **Source system** | `depm` |
| **Source table** | `depm_r100_temp` |
| **Materialization** | `incremental` |
| **Primary keys** | `version`, `project`, `wbs`, `riskid`, `riskactionid` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `version` | `string` | `varchar` | `PK` |  | From table dEPM_R100_Temp and column Version. Max length: 100 |
| `project` | `string` | `varchar` | `PK` |  | From table dEPM_R100_Temp and column Project. Max length: 100 |
| `wbs` | `string` | `varchar` | `PK` |  | From table dEPM_R100_Temp and column WBS. Max length: 100 |
| `category` | `string` | `varchar` |  |  | From table dEPM_R100_Temp and column Category. Max length: 100 |
| `riskid` | `string` | `varchar` | `PK` |  | From table dEPM_R100_Temp and column RiskID. Max length: 100 |
| `riskactionid` | `string` | `varchar` | `PK` |  | From table dEPM_R100_Temp and column RiskActionID. Max length: 100 |
| `measure` | `string` | `varchar` |  |  | From table dEPM_R100_Temp and column Measure. Max length: 100 |
| `value` | `string` | `varchar` |  |  | From table dEPM_R100_Temp and column Value. Max length: 100 |
| `timestamp` | `timestamp` | `timestamp_ntz` |  | `not_null` | (required) From table dEPM_R100_Temp and column Timestamp. Max length: 24 |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

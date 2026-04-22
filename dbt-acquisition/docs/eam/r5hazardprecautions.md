# r5hazardprecautions

eam_R5HAZARDPRECAUTIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5hazardprecautions` |
| **Materialization** | `incremental` |
| **Primary keys** | `hzp_hazard`, `hzp_hazard_org`, `hzp_revision`, `hzp_precaution`, `hzp_precaution_org` |
| **Column count** | 14 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `hzp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | HZP_LASTSAVED |
| `hzp_hazard_org` | `string` | `varchar` | `PK` |  | HZP_HAZARD_ORG |
| `hzp_revision` | `float` | `float` | `PK` |  | HZP_REVISION |
| `hzp_precaution_org` | `string` | `varchar` | `PK` |  | HZP_PRECAUTION_ORG |
| `hzp_sequence` | `float` | `float` |  |  | HZP_SEQUENCE |
| `hzp_updatecount` | `float` | `float` |  |  | HZP_UPDATECOUNT |
| `hzp_hazard` | `string` | `varchar` | `PK` |  | HZP_HAZARD |
| `hzp_precaution` | `string` | `varchar` | `PK` |  | HZP_PRECAUTION |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

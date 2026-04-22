# r5actioncodes

eam_R5ACTIONCODES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5actioncodes` |
| **Materialization** | `incremental` |
| **Primary keys** | `acc_code` |
| **Column count** | 19 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `acc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ACC_LASTSAVED |
| `acc_desc` | `string` | `varchar` |  |  | ACC_DESC |
| `acc_class` | `string` | `varchar` |  |  | ACC_CLASS |
| `acc_gen` | `string` | `varchar` |  |  | ACC_GEN |
| `acc_class_org` | `string` | `varchar` |  |  | ACC_CLASS_ORG |
| `acc_updatecount` | `float` | `float` |  |  | ACC_UPDATECOUNT |
| `acc_updated` | `timestamp` | `timestamp_ntz` |  |  | ACC_UPDATED |
| `acc_partfailure` | `string` | `varchar` |  |  | ACC_PARTFAILURE |
| `acc_notused` | `string` | `varchar` |  |  | ACC_NOTUSED |
| `acc_enableworkorders` | `string` | `varchar` |  |  | ACC_ENABLEWORKORDERS |
| `acc_group` | `string` | `varchar` |  |  | ACC_GROUP |
| `acc_code` | `string` | `varchar` | `PK` | `unique` | ACC_CODE |
| `acc_created` | `timestamp` | `timestamp_ntz` |  |  | ACC_CREATED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

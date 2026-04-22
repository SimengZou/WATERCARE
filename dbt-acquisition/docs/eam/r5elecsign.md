# r5elecsign

eam_R5ELECSIGN

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5elecsign` |
| **Materialization** | `incremental` |
| **Primary keys** | `els_code` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `els_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ELS_LASTSAVED |
| `els_org` | `string` | `varchar` |  |  | ELS_ORG |
| `els_user` | `string` | `varchar` |  |  | ELS_USER |
| `els_date` | `timestamp` | `timestamp_ntz` |  |  | ELS_DATE |
| `els_entity` | `string` | `varchar` |  |  | ELS_ENTITY |
| `els_signtype` | `string` | `varchar` |  |  | ELS_SIGNTYPE |
| `els_status` | `string` | `varchar` |  |  | ELS_STATUS |
| `els_parent` | `string` | `varchar` |  |  | ELS_PARENT |
| `els_certifynum` | `string` | `varchar` |  |  | ELS_CERTIFYNUM |
| `els_certifytype` | `string` | `varchar` |  |  | ELS_CERTIFYTYPE |
| `els_externaldate` | `timestamp` | `timestamp_ntz` |  |  | ELS_EXTERNALDATE |
| `els_userdesc` | `string` | `varchar` |  |  | ELS_USERDESC |
| `els_signtypedesc` | `string` | `varchar` |  |  | ELS_SIGNTYPEDESC |
| `els_lastchanged` | `string` | `varchar` |  |  | ELS_LASTCHANGED |
| `els_entcode2` | `string` | `varchar` |  |  | ELS_ENTCODE2 |
| `els_code` | `string` | `varchar` | `PK` | `unique` | ELS_CODE |
| `els_entcode` | `string` | `varchar` |  |  | ELS_ENTCODE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

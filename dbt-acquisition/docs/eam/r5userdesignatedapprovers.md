# r5userdesignatedapprovers

eam_R5USERDESIGNATEDAPPROVERS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5userdesignatedapprovers` |
| **Materialization** | `incremental` |
| **Primary keys** | `uda_pk` |
| **Column count** | 43 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `uda_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | UDA_LASTSAVED |
| `uda_designatedapprover` | `string` | `varchar` |  |  | UDA_DESIGNATEDAPPROVER |
| `uda_status` | `string` | `varchar` |  |  | UDA_STATUS |
| `uda_rstatus` | `string` | `varchar` |  |  | UDA_RSTATUS |
| `uda_startdate` | `timestamp` | `timestamp_ntz` |  |  | UDA_STARTDATE |
| `uda_enddate` | `timestamp` | `timestamp_ntz` |  |  | UDA_ENDDATE |
| `uda_reason` | `string` | `varchar` |  |  | UDA_REASON |
| `uda_notes` | `string` | `varchar` |  |  | UDA_NOTES |
| `uda_approvedby` | `string` | `varchar` |  |  | UDA_APPROVEDBY |
| `uda_approved` | `timestamp` | `timestamp_ntz` |  |  | UDA_APPROVED |
| `uda_createdby` | `string` | `varchar` |  |  | UDA_CREATEDBY |
| `uda_created` | `timestamp` | `timestamp_ntz` |  |  | UDA_CREATED |
| `uda_updatedby` | `string` | `varchar` |  |  | UDA_UPDATEDBY |
| `uda_updated` | `timestamp` | `timestamp_ntz` |  |  | UDA_UPDATED |
| `uda_udfchar01` | `string` | `varchar` |  |  | UDA_UDFCHAR01 |
| `uda_udfchar02` | `string` | `varchar` |  |  | UDA_UDFCHAR02 |
| `uda_udfchar03` | `string` | `varchar` |  |  | UDA_UDFCHAR03 |
| `uda_udfchar04` | `string` | `varchar` |  |  | UDA_UDFCHAR04 |
| `uda_udfchar05` | `string` | `varchar` |  |  | UDA_UDFCHAR05 |
| `uda_udfnum01` | `float` | `float` |  |  | UDA_UDFNUM01 |
| `uda_udfnum02` | `float` | `float` |  |  | UDA_UDFNUM02 |
| `uda_udfnum03` | `float` | `float` |  |  | UDA_UDFNUM03 |
| `uda_udfnum04` | `float` | `float` |  |  | UDA_UDFNUM04 |
| `uda_udfnum05` | `float` | `float` |  |  | UDA_UDFNUM05 |
| `uda_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | UDA_UDFDATE01 |
| `uda_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | UDA_UDFDATE02 |
| `uda_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | UDA_UDFDATE03 |
| `uda_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | UDA_UDFDATE04 |
| `uda_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | UDA_UDFDATE05 |
| `uda_udfchkbox01` | `string` | `varchar` |  |  | UDA_UDFCHKBOX01 |
| `uda_udfchkbox02` | `string` | `varchar` |  |  | UDA_UDFCHKBOX02 |
| `uda_udfchkbox03` | `string` | `varchar` |  |  | UDA_UDFCHKBOX03 |
| `uda_udfchkbox04` | `string` | `varchar` |  |  | UDA_UDFCHKBOX04 |
| `uda_udfchkbox05` | `string` | `varchar` |  |  | UDA_UDFCHKBOX05 |
| `uda_updatecount` | `float` | `float` |  |  | UDA_UPDATECOUNT |
| `uda_pk` | `string` | `varchar` | `PK` | `unique` | UDA_PK |
| `uda_user` | `string` | `varchar` |  |  | UDA_USER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

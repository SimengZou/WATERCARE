# r5nonconformitysetup

eam_R5NONCONFORMITYSETUP

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5nonconformitysetup` |
| **Materialization** | `incremental` |
| **Primary keys** | `ncp_org` |
| **Column count** | 31 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ncp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | NCP_LASTSAVED |
| `ncp_copyfrom` | `string` | `varchar` |  |  | NCP_COPYFROM |
| `ncp_protectobsdataonhdr` | `string` | `varchar` |  |  | NCP_PROTECTOBSDATAONHDR |
| `ncp_synchronize` | `string` | `varchar` |  |  | NCP_SYNCHRONIZE |
| `ncp_mergerestrictions` | `string` | `varchar` |  |  | NCP_MERGERESTRICTIONS |
| `ncp_updatedby` | `string` | `varchar` |  |  | NCP_UPDATEDBY |
| `ncp_updated` | `timestamp` | `timestamp_ntz` |  |  | NCP_UPDATED |
| `ncp_useobsstatus` | `string` | `varchar` |  |  | NCP_USEOBSSTATUS |
| `ncp_supersededstatus` | `string` | `varchar` |  |  | NCP_SUPERSEDEDSTATUS |
| `ncp_reinspectstatus` | `string` | `varchar` |  |  | NCP_REINSPECTSTATUS |
| `ncp_autoapprovestatus` | `string` | `varchar` |  |  | NCP_AUTOAPPROVESTATUS |
| `ncp_autoskipstatus` | `string` | `varchar` |  |  | NCP_AUTOSKIPSTATUS |
| `ncp_includecondition` | `string` | `varchar` |  |  | NCP_INCLUDECONDITION |
| `ncp_prevobscount` | `float` | `float` |  |  | NCP_PREVOBSCOUNT |
| `ncp_updatecount` | `float` | `float` |  |  | NCP_UPDATECOUNT |
| `ncp_createfromchecklist` | `string` | `varchar` |  |  | NCP_CREATEFROMCHECKLIST |
| `ncp_massacknowledgeallowed` | `string` | `varchar` |  |  | NCP_MASSACKNOWLEDGEALLOWED |
| `ncp_copyseverity` | `string` | `varchar` |  |  | NCP_COPYSEVERITY |
| `ncp_copyintensity` | `string` | `varchar` |  |  | NCP_COPYINTENSITY |
| `ncp_copysize` | `string` | `varchar` |  |  | NCP_COPYSIZE |
| `ncp_copyimportance` | `string` | `varchar` |  |  | NCP_COPYIMPORTANCE |
| `ncp_copynextinspdateoverride` | `string` | `varchar` |  |  | NCP_COPYNEXTINSPDATEOVERRIDE |
| `ncp_copyobservationudfs` | `string` | `varchar` |  |  | NCP_COPYOBSERVATIONUDFS |
| `ncp_org` | `string` | `varchar` | `PK` | `unique` | NCP_ORG |
| `ncp_protectheader` | `string` | `varchar` |  |  | NCP_PROTECTHEADER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

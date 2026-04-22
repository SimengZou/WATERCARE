# r5productionrequests

eam_R5PRODUCTIONREQUESTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5productionrequests` |
| **Materialization** | `incremental` |
| **Primary keys** | `prq_code`, `prq_revision`, `prq_org` |
| **Column count** | 29 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `prq_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PRQ_LASTSAVED |
| `prq_org` | `string` | `varchar` | `PK` |  | PRQ_ORG |
| `prq_class` | `string` | `varchar` |  |  | PRQ_CLASS |
| `prq_class_org` | `string` | `varchar` |  |  | PRQ_CLASS_ORG |
| `prq_accountingentity` | `string` | `varchar` |  |  | PRQ_ACCOUNTINGENTITY |
| `prq_status` | `string` | `varchar` |  |  | PRQ_STATUS |
| `prq_rstatus` | `string` | `varchar` |  |  | PRQ_RSTATUS |
| `prq_event` | `string` | `varchar` |  |  | PRQ_EVENT |
| `prq_created` | `timestamp` | `timestamp_ntz` |  |  | PRQ_CREATED |
| `prq_createdby` | `string` | `varchar` |  |  | PRQ_CREATEDBY |
| `prq_prodrequeststart` | `timestamp` | `timestamp_ntz` |  |  | PRQ_PRODREQUESTSTART |
| `prq_prodrequestend` | `timestamp` | `timestamp_ntz` |  |  | PRQ_PRODREQUESTEND |
| `prq_productionstart` | `timestamp` | `timestamp_ntz` |  |  | PRQ_PRODUCTIONSTART |
| `prq_productionend` | `timestamp` | `timestamp_ntz` |  |  | PRQ_PRODUCTIONEND |
| `prq_priority` | `string` | `varchar` |  |  | PRQ_PRIORITY |
| `prq_productionorder` | `string` | `varchar` |  |  | PRQ_PRODUCTIONORDER |
| `prq_enterpriselocation` | `string` | `varchar` |  |  | PRQ_ENTERPRISELOCATION |
| `prq_laststatusupdate` | `timestamp` | `timestamp_ntz` |  |  | PRQ_LASTSTATUSUPDATE |
| `prq_updatecount` | `float` | `float` |  |  | PRQ_UPDATECOUNT |
| `prq_revisionreason` | `string` | `varchar` |  |  | PRQ_REVISIONREASON |
| `prq_code` | `string` | `varchar` | `PK` |  | PRQ_CODE |
| `prq_revision` | `float` | `float` | `PK` |  | PRQ_REVISION |
| `prq_desc` | `string` | `varchar` |  |  | PRQ_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

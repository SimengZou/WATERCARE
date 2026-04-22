# u5servicerequest

eam_U5SERVICEREQUEST

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5servicerequest` |
| **Materialization** | `incremental` |
| **Primary keys** | `crm_transactionid` |
| **Column count** | 34 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `crm_problemcode` | `string` | `varchar` |  |  | CRM_PROBLEMCODE |
| `crm_servicerequest` | `string` | `varchar` |  |  | CRM_SERVICEREQUEST |
| `crm_priority` | `string` | `varchar` |  |  | CRM_PRIORITY |
| `crm_addresskey` | `string` | `varchar` |  |  | CRM_ADDRESSKEY |
| `crm_flatno` | `string` | `varchar` |  |  | CRM_FLATNO |
| `crm_streetno` | `string` | `varchar` |  |  | CRM_STREETNO |
| `crm_stname` | `string` | `varchar` |  |  | CRM_STNAME |
| `crm_suburb` | `string` | `varchar` |  |  | CRM_SUBURB |
| `crm_city` | `string` | `varchar` |  |  | CRM_CITY |
| `crm_postalcode` | `string` | `varchar` |  |  | CRM_POSTALCODE |
| `crm_notes` | `string` | `varchar` |  |  | CRM_NOTES. Max length: 0 |
| `crm_reporteddate` | `timestamp` | `timestamp_ntz` |  |  | CRM_REPORTEDDATE |
| `crm_calltakenby` | `string` | `varchar` |  |  | CRM_CALLTAKENBY |
| `crm_callerfistname` | `string` | `varchar` |  |  | CRM_CALLERFISTNAME |
| `crm_callerlastname` | `string` | `varchar` |  |  | CRM_CALLERLASTNAME |
| `crm_contactnumber` | `string` | `varchar` |  |  | CRM_CONTACTNUMBER |
| `crm_alternatecontact` | `string` | `varchar` |  |  | CRM_ALTERNATECONTACT |
| `crm_contactemail` | `string` | `varchar` |  |  | CRM_CONTACTEMAIL |
| `crm_contactrequested` | `string` | `varchar` |  |  | CRM_CONTACTREQUESTED |
| `crm_event` | `string` | `varchar` |  |  | CRM_EVENT |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `crm_source` | `string` | `varchar` |  |  | CRM_SOURCE |
| `crm_transactionid` | `string` | `varchar` | `PK` | `unique` | CRM_TRANSACTIONID |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

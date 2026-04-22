# u5woclaims

eam_U5WOCLAIMS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_u5woclaims` |
| **Materialization** | `incremental` |
| **Primary keys** | `wco_event`, `wco_pk`, `wco_org` |
| **Column count** | 40 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `lastsaved` | `timestamp` | `timestamp_ntz` |  |  | LASTSAVED |
| `wco_trans_flag` | `string` | `varchar` |  |  | WCO_TRANS_FLAG |
| `wco_org` | `string` | `varchar` | `PK` |  | WCO_ORG |
| `wco_schedule_item` | `string` | `varchar` |  |  | WCO_SCHEDULE_ITEM |
| `wco_transid` | `string` | `varchar` |  |  | WCO_TRANSID |
| `wco_contractor_qty` | `float` | `float` |  |  | WCO_CONTRACTOR_QTY |
| `wco_contractor_rate` | `float` | `float` |  |  | WCO_CONTRACTOR_RATE |
| `wco_chargedate` | `timestamp` | `timestamp_ntz` |  |  | WCO_CHARGEDATE |
| `wco_comments` | `string` | `varchar` |  |  | WCO_COMMENTS |
| `wco_scheditem_desc` | `string` | `varchar` |  |  | WCO_SCHEDITEM_DESC |
| `wco_scheditem_rate` | `float` | `float` |  |  | WCO_SCHEDITEM_RATE |
| `wco_linetotal` | `float` | `float` |  |  | WCO_LINETOTAL |
| `wco_woscheditem` | `string` | `varchar` |  |  | WCO_WOSCHEDITEM |
| `wco_wotype` | `string` | `varchar` |  |  | WCO_WOTYPE |
| `wco_woparent` | `string` | `varchar` |  |  | WCO_WOPARENT |
| `wco_contract_code` | `string` | `varchar` |  |  | WCO_CONTRACT_CODE |
| `wco_contractor` | `string` | `varchar` |  |  | WCO_CONTRACTOR |
| `wco_activity` | `string` | `varchar` |  |  | WCO_ACTIVITY |
| `wco_activity_desc` | `string` | `varchar` |  |  | WCO_ACTIVITY_DESC |
| `createdby` | `string` | `varchar` |  |  | CREATEDBY |
| `created` | `timestamp` | `timestamp_ntz` |  |  | CREATED |
| `updatedby` | `string` | `varchar` |  |  | UPDATEDBY |
| `updated` | `timestamp` | `timestamp_ntz` |  |  | UPDATED |
| `updatecount` | `float` | `float` |  |  | UPDATECOUNT |
| `wco_line_status` | `string` | `varchar` |  |  | WCO_LINE_STATUS |
| `wco_comments_int` | `string` | `varchar` |  |  | WCO_COMMENTS_INT |
| `wco_derprojact` | `string` | `varchar` |  |  | WCO_DERPROJACT |
| `wco_derprojnum` | `string` | `varchar` |  |  | WCO_DERPROJNUM |
| `wco_expprojact` | `string` | `varchar` |  |  | WCO_EXPPROJACT |
| `wco_expprojnum` | `string` | `varchar` |  |  | WCO_EXPPROJNUM |
| `wco_processed_error` | `string` | `varchar` |  |  | WCO_PROCESSED_ERROR. Max length: 0 |
| `wco_processed_status` | `string` | `varchar` |  |  | WCO_PROCESSED_STATUS |
| `wco_event` | `string` | `varchar` | `PK` |  | WCO_EVENT |
| `wco_pk` | `string` | `varchar` | `PK` |  | WCO_PK |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

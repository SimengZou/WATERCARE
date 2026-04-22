# r5contracts

eam_R5CONTRACTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5contracts` |
| **Materialization** | `incremental` |
| **Primary keys** | `con_code` |
| **Column count** | 30 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `con_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CON_LASTSAVED |
| `con_class` | `string` | `varchar` |  |  | CON_CLASS |
| `con_supplier` | `string` | `varchar` |  |  | CON_SUPPLIER |
| `con_lang` | `string` | `varchar` |  |  | CON_LANG |
| `con_curr` | `string` | `varchar` |  |  | CON_CURR |
| `con_copy` | `string` | `varchar` |  |  | CON_COPY |
| `con_store` | `string` | `varchar` |  |  | CON_STORE |
| `con_status` | `string` | `varchar` |  |  | CON_STATUS |
| `con_rstatus` | `string` | `varchar` |  |  | CON_RSTATUS |
| `con_own` | `string` | `varchar` |  |  | CON_OWN |
| `con_ref` | `string` | `varchar` |  |  | CON_REF |
| `con_create` | `timestamp` | `timestamp_ntz` |  |  | CON_CREATE |
| `con_start` | `timestamp` | `timestamp_ntz` |  |  | CON_START |
| `con_end` | `timestamp` | `timestamp_ntz` |  |  | CON_END |
| `con_renew` | `timestamp` | `timestamp_ntz` |  |  | CON_RENEW |
| `con_printed` | `string` | `varchar` |  |  | CON_PRINTED |
| `con_owncontact` | `string` | `varchar` |  |  | CON_OWNCONTACT |
| `con_contact` | `string` | `varchar` |  |  | CON_CONTACT |
| `con_org` | `string` | `varchar` |  |  | CON_ORG |
| `con_class_org` | `string` | `varchar` |  |  | CON_CLASS_ORG |
| `con_supplier_org` | `string` | `varchar` |  |  | CON_SUPPLIER_ORG |
| `con_updatecount` | `float` | `float` |  |  | CON_UPDATECOUNT |
| `con_code` | `string` | `varchar` | `PK` | `unique` | CON_CODE |
| `con_desc` | `string` | `varchar` |  |  | CON_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

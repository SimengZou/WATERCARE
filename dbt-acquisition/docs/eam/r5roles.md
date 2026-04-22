# r5roles

eam_R5ROLES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5roles` |
| **Materialization** | `incremental` |
| **Primary keys** | `rol_code` |
| **Column count** | 37 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rol_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ROL_LASTSAVED |
| `rol_group` | `string` | `varchar` |  |  | ROL_GROUP |
| `rol_defaultorg` | `string` | `varchar` |  |  | ROL_DEFAULTORG |
| `rol_lang` | `string` | `varchar` |  |  | ROL_LANG |
| `rol_successmsgtimeout` | `float` | `float` |  |  | ROL_SUCCESSMSGTIMEOUT |
| `rol_locale` | `string` | `varchar` |  |  | ROL_LOCALE |
| `rol_mrc` | `string` | `varchar` |  |  | ROL_MRC |
| `rol_firstfunc` | `string` | `varchar` |  |  | ROL_FIRSTFUNC |
| `rol_ewsfirstfunc` | `string` | `varchar` |  |  | ROL_EWSFIRSTFUNC |
| `rol_reqappvlimit` | `float` | `float` |  |  | ROL_REQAPPVLIMIT |
| `rol_reqauthappvlimit` | `float` | `float` |  |  | ROL_REQAUTHAPPVLIMIT |
| `rol_invappvlimit` | `float` | `float` |  |  | ROL_INVAPPVLIMIT |
| `rol_invappvlimitnonpo` | `float` | `float` |  |  | ROL_INVAPPVLIMITNONPO |
| `rol_pordappvlimit` | `float` | `float` |  |  | ROL_PORDAPPVLIMIT |
| `rol_pordauthappvlimit` | `float` | `float` |  |  | ROL_PORDAUTHAPPVLIMIT |
| `rol_picappvlimit` | `float` | `float` |  |  | ROL_PICAPPVLIMIT |
| `rol_buyer` | `string` | `varchar` |  |  | ROL_BUYER |
| `rol_approver` | `string` | `varchar` |  |  | ROL_APPROVER |
| `rol_requestor` | `string` | `varchar` |  |  | ROL_REQUESTOR |
| `rol_router` | `string` | `varchar` |  |  | ROL_ROUTER |
| `rol_updatecount` | `float` | `float` |  |  | ROL_UPDATECOUNT |
| `rol_active` | `string` | `varchar` |  |  | ROL_ACTIVE |
| `rol_mobile` | `string` | `varchar` |  |  | ROL_MOBILE |
| `rol_connector` | `string` | `varchar` |  |  | ROL_CONNECTOR |
| `rol_barcode` | `string` | `varchar` |  |  | ROL_BARCODE |
| `rol_analytics` | `string` | `varchar` |  |  | ROL_ANALYTICS |
| `rol_advrepauthor` | `string` | `varchar` |  |  | ROL_ADVREPAUTHOR |
| `rol_advrepconsumer` | `string` | `varchar` |  |  | ROL_ADVREPCONSUMER |
| `rol_mobileadm` | `string` | `varchar` |  |  | ROL_MOBILEADM |
| `rol_code` | `string` | `varchar` | `PK` | `unique` | ROL_CODE |
| `rol_desc` | `string` | `varchar` |  |  | ROL_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

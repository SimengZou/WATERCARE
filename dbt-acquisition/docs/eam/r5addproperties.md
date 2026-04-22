# r5addproperties

eam_R5ADDPROPERTIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5addproperties` |
| **Materialization** | `incremental` |
| **Primary keys** | `apr_property`, `apr_rentity`, `apr_class`, `apr_class_org` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `apr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | APR_LASTSAVED |
| `apr_nonupdcat` | `string` | `varchar` |  |  | APR_NONUPDCAT |
| `apr_created` | `timestamp` | `timestamp_ntz` |  |  | APR_CREATED |
| `apr_updated` | `timestamp` | `timestamp_ntz` |  |  | APR_UPDATED |
| `apr_grouplabel` | `string` | `varchar` |  |  | APR_GROUPLABEL |
| `apr_status` | `string` | `varchar` |  |  | APR_STATUS |
| `apr_class_org` | `string` | `varchar` | `PK` |  | APR_CLASS_ORG |
| `apr_property` | `string` | `varchar` | `PK` |  | APR_PROPERTY |
| `apr_rentity` | `string` | `varchar` | `PK` |  | APR_RENTITY |
| `apr_class` | `string` | `varchar` | `PK` |  | APR_CLASS |
| `apr_line` | `float` | `float` |  |  | APR_LINE |
| `apr_uom` | `string` | `varchar` |  |  | APR_UOM |
| `apr_list` | `string` | `varchar` |  |  | APR_LIST |
| `apr_listvalid` | `string` | `varchar` |  |  | APR_LISTVALID |
| `apr_required` | `string` | `varchar` |  |  | APR_REQUIRED |
| `apr_wodisp` | `string` | `varchar` |  |  | APR_WODISP |
| `apr_updatecount` | `float` | `float` |  |  | APR_UPDATECOUNT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

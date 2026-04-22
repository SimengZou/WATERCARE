# r5propertyvalues

eam_R5PROPERTYVALUES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5propertyvalues` |
| **Materialization** | `incremental` |
| **Primary keys** | `prv_property`, `prv_rentity`, `prv_class`, `prv_class_org`, `prv_code`, `prv_seqno` |
| **Column count** | 20 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `prv_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PRV_LASTSAVED |
| `prv_rentity` | `string` | `varchar` | `PK` |  | PRV_RENTITY |
| `prv_class` | `string` | `varchar` | `PK` |  | PRV_CLASS |
| `prv_code` | `string` | `varchar` | `PK` |  | PRV_CODE |
| `prv_seqno` | `float` | `float` | `PK` |  | PRV_SEQNO |
| `prv_value` | `string` | `varchar` |  |  | PRV_VALUE |
| `prv_dvalue` | `timestamp` | `timestamp_ntz` |  |  | PRV_DVALUE |
| `prv_class_org` | `string` | `varchar` | `PK` |  | PRV_CLASS_ORG |
| `prv_updatecount` | `float` | `float` |  |  | PRV_UPDATECOUNT |
| `prv_created` | `timestamp` | `timestamp_ntz` |  |  | PRV_CREATED |
| `prv_updated` | `timestamp` | `timestamp_ntz` |  |  | PRV_UPDATED |
| `prv_notused` | `string` | `varchar` |  |  | PRV_NOTUSED |
| `prv_property` | `string` | `varchar` | `PK` |  | PRV_PROPERTY |
| `prv_nvalue` | `float` | `float` |  |  | PRV_NVALUE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

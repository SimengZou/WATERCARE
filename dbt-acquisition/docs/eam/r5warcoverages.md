# r5warcoverages

eam_R5WARCOVERAGES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5warcoverages` |
| **Materialization** | `incremental` |
| **Primary keys** | `wcv_seqno` |
| **Column count** | 26 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wcv_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WCV_LASTSAVED |
| `wcv_obtype` | `string` | `varchar` |  |  | WCV_OBTYPE |
| `wcv_obrtype` | `string` | `varchar` |  |  | WCV_OBRTYPE |
| `wcv_uom` | `string` | `varchar` |  |  | WCV_UOM |
| `wcv_duration` | `float` | `float` |  |  | WCV_DURATION |
| `wcv_expiration` | `float` | `float` |  |  | WCV_EXPIRATION |
| `wcv_expirationdate` | `timestamp` | `timestamp_ntz` |  |  | WCV_EXPIRATIONDATE |
| `wcv_nearthreshold` | `float` | `float` |  |  | WCV_NEARTHRESHOLD |
| `wcv_startusage` | `float` | `float` |  |  | WCV_STARTUSAGE |
| `wcv_seqno` | `float` | `float` | `PK` | `unique` | WCV_SEQNO |
| `wcv_active` | `string` | `varchar` |  |  | WCV_ACTIVE |
| `wcv_startdate` | `timestamp` | `timestamp_ntz` |  |  | WCV_STARTDATE |
| `wcv_recorded` | `timestamp` | `timestamp_ntz` |  |  | WCV_RECORDED |
| `wcv_user` | `string` | `varchar` |  |  | WCV_USER |
| `wcv_object_org` | `string` | `varchar` |  |  | WCV_OBJECT_ORG |
| `wcv_updatecount` | `float` | `float` |  |  | WCV_UPDATECOUNT |
| `wcv_created` | `timestamp` | `timestamp_ntz` |  |  | WCV_CREATED |
| `wcv_updated` | `timestamp` | `timestamp_ntz` |  |  | WCV_UPDATED |
| `wcv_warranty` | `string` | `varchar` |  |  | WCV_WARRANTY |
| `wcv_object` | `string` | `varchar` |  |  | WCV_OBJECT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

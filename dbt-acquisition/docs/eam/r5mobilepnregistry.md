# r5mobilepnregistry

eam_R5MOBILEPNREGISTRY

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mobilepnregistry` |
| **Materialization** | `incremental` |
| **Primary keys** | `mpn_appid`, `mpn_deviceid` |
| **Column count** | 17 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `mpn_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MPN_LASTSAVED |
| `mpn_deviceid` | `string` | `varchar` | `PK` |  | MPN_DEVICEID |
| `mpn_platform` | `string` | `varchar` |  |  | MPN_PLATFORM |
| `mpn_token` | `string` | `varchar` |  |  | MPN_TOKEN. Max length: 0 |
| `mpn_user` | `string` | `varchar` |  |  | MPN_USER |
| `mpn_createdby` | `string` | `varchar` |  |  | MPN_CREATEDBY |
| `mpn_updated` | `timestamp` | `timestamp_ntz` |  |  | MPN_UPDATED |
| `mpn_updatedby` | `string` | `varchar` |  |  | MPN_UPDATEDBY |
| `mpn_updatecount` | `float` | `float` |  |  | MPN_UPDATECOUNT |
| `mpn_appid` | `string` | `varchar` | `PK` |  | MPN_APPID |
| `mpn_created` | `timestamp` | `timestamp_ntz` |  |  | MPN_CREATED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

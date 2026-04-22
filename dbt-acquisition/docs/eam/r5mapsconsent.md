# r5mapsconsent

eam_R5MAPSCONSENT

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mapsconsent` |
| **Materialization** | `incremental` |
| **Primary keys** | `mct_appid`, `mct_userid`, `mct_deviceid`, `mct_product` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `mct_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MCT_LASTSAVED |
| `mct_userid` | `string` | `varchar` | `PK` |  | MCT_USERID |
| `mct_deviceid` | `string` | `varchar` | `PK` |  | MCT_DEVICEID |
| `mct_lastupdated` | `timestamp` | `timestamp_ntz` |  |  | MCT_LASTUPDATED |
| `mct_updatecount` | `float` | `float` |  |  | MCT_UPDATECOUNT |
| `mct_appid` | `string` | `varchar` | `PK` |  | MCT_APPID |
| `mct_product` | `string` | `varchar` | `PK` |  | MCT_PRODUCT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

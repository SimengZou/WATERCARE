# r5mobiledbsync

eam_R5MOBILEDBSYNC

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5mobiledbsync` |
| **Materialization** | `incremental` |
| **Primary keys** | `mds_syncid` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `mds_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MDS_LASTSAVED |
| `mds_user` | `string` | `varchar` |  |  | MDS_USER |
| `mds_org` | `string` | `varchar` |  |  | MDS_ORG |
| `mds_status` | `string` | `varchar` |  |  | MDS_STATUS |
| `mds_dblastupdateddate` | `timestamp` | `timestamp_ntz` |  |  | MDS_DBLASTUPDATEDDATE |
| `mds_status_msg` | `string` | `varchar` |  |  | MDS_STATUS_MSG |
| `mds_sync_request` | `string` | `varchar` |  |  | MDS_SYNC_REQUEST. Max length: 0 |
| `mds_download` | `string` | `varchar` |  |  | MDS_DOWNLOAD |
| `mds_filename` | `string` | `varchar` |  |  | MDS_FILENAME |
| `mds_updatecount` | `float` | `float` |  |  | MDS_UPDATECOUNT |
| `mds_syncid` | `float` | `float` | `PK` | `unique` | MDS_SYNCID |
| `mds_grids_processed` | `float` | `float` |  |  | MDS_GRIDS_PROCESSED |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5chargedefsequence

eam_R5CHARGEDEFSEQUENCE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5chargedefsequence` |
| **Materialization** | `incremental` |
| **Primary keys** | `cds_category`, `cds_subcategory`, `cds_level`, `cds_sequence` |
| **Column count** | 13 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `cds_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CDS_LASTSAVED |
| `cds_subcategory` | `string` | `varchar` | `PK` |  | CDS_SUBCATEGORY |
| `cds_level` | `string` | `varchar` | `PK` |  | CDS_LEVEL |
| `cds_sequence` | `float` | `float` | `PK` |  | CDS_SEQUENCE |
| `cds_updatecount` | `float` | `float` |  |  | CDS_UPDATECOUNT |
| `cds_category` | `string` | `varchar` | `PK` |  | CDS_CATEGORY |
| `cds_actualsubcat` | `string` | `varchar` |  |  | CDS_ACTUALSUBCAT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5uoms

eam_R5UOMS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5uoms` |
| **Materialization** | `incremental` |
| **Primary keys** | `uom_code` |
| **Column count** | 16 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `uom_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | UOM_LASTSAVED |
| `uom_desc` | `string` | `varchar` |  |  | UOM_DESC |
| `uom_class` | `string` | `varchar` |  |  | UOM_CLASS |
| `uom_notused` | `string` | `varchar` |  |  | UOM_NOTUSED |
| `uom_updatecount` | `float` | `float` |  |  | UOM_UPDATECOUNT |
| `uom_created` | `timestamp` | `timestamp_ntz` |  |  | UOM_CREATED |
| `uom_updated` | `timestamp` | `timestamp_ntz` |  |  | UOM_UPDATED |
| `uom_soauom` | `string` | `varchar` |  |  | UOM_SOAUOM |
| `uom_code` | `string` | `varchar` | `PK` | `unique` | UOM_CODE |
| `uom_class_org` | `string` | `varchar` |  |  | UOM_CLASS_ORG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

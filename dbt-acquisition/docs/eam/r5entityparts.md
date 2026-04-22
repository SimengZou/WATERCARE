# r5entityparts

eam_R5ENTITYPARTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5entityparts` |
| **Materialization** | `incremental` |
| **Primary keys** | `epa_pk` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `epa_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | EPA_LASTSAVED |
| `epa_rentity` | `string` | `varchar` |  |  | EPA_RENTITY |
| `epa_type` | `string` | `varchar` |  |  | EPA_TYPE |
| `epa_rtype` | `string` | `varchar` |  |  | EPA_RTYPE |
| `epa_code` | `string` | `varchar` |  |  | EPA_CODE |
| `epa_qty` | `float` | `float` |  |  | EPA_QTY |
| `epa_uom` | `string` | `varchar` |  |  | EPA_UOM |
| `epa_comment` | `string` | `varchar` |  |  | EPA_COMMENT |
| `epa_part_org` | `string` | `varchar` |  |  | EPA_PART_ORG |
| `epa_updatecount` | `float` | `float` |  |  | EPA_UPDATECOUNT |
| `epa_pk` | `string` | `varchar` | `PK` | `unique` | EPA_PK |
| `epa_partlocation` | `string` | `varchar` |  |  | EPA_PARTLOCATION |
| `epa_syslevel` | `string` | `varchar` |  |  | EPA_SYSLEVEL |
| `epa_asmlevel` | `string` | `varchar` |  |  | EPA_ASMLEVEL |
| `epa_complevel` | `string` | `varchar` |  |  | EPA_COMPLEVEL |
| `epa_part` | `string` | `varchar` |  |  | EPA_PART |
| `epa_entity` | `string` | `varchar` |  |  | EPA_ENTITY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

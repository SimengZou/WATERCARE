# r5bvdwmroinvpartstobystore

eam_R5BVDWMROINVPARTSTOBYSTORE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5bvdwmroinvpartstobystore` |
| **Materialization** | `incremental` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `zdt_key` | `float` | `float` |  |  | ZDT_KEY |
| `zor_key` | `float` | `float` |  |  | ZOR_KEY |
| `zso_key` | `float` | `float` |  |  | ZSO_KEY |
| `mro_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MRO_LASTSAVED |
| `mro_orderedmatlcostdefcur` | `float` | `float` |  |  | MRO_ORDEREDMATLCOSTDEFCUR |
| `mro_valueonhanddefcur` | `float` | `float` |  |  | MRO_VALUEONHANDDEFCUR |
| `mro_materialcostdefcur` | `float` | `float` |  |  | MRO_MATERIALCOSTDEFCUR |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

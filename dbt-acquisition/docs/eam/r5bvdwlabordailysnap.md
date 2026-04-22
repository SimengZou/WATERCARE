# r5bvdwlabordailysnap

eam_R5BVDWLABORDAILYSNAP

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5bvdwlabordailysnap` |
| **Materialization** | `incremental` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `zdt_key` | `float` | `float` |  |  | ZDT_KEY |
| `zwo_key` | `float` | `float` |  |  | ZWO_KEY |
| `zpe_key` | `float` | `float` |  |  | ZPE_KEY |
| `ztr_key` | `float` | `float` |  |  | ZTR_KEY |
| `zmr_key` | `float` | `float` |  |  | ZMR_KEY |
| `zot_key` | `float` | `float` |  |  | ZOT_KEY |
| `zor_key` | `float` | `float` |  |  | ZOR_KEY |
| `zlb_hours` | `float` | `float` |  |  | ZLB_HOURS |
| `zlb_costdefcur` | `float` | `float` |  |  | ZLB_COSTDEFCUR |
| `zlb_defcur` | `string` | `varchar` |  |  | ZLB_DEFCUR |
| `zlb_costorgcur` | `float` | `float` |  |  | ZLB_COSTORGCUR |
| `zlb_orgcur` | `string` | `varchar` |  |  | ZLB_ORGCUR |
| `zlb_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ZLB_LASTSAVED |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

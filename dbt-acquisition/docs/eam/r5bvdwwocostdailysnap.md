# r5bvdwwocostdailysnap

eam_R5BVDWWOCOSTDAILYSNAP

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5bvdwwocostdailysnap` |
| **Materialization** | `incremental` |
| **Column count** | 27 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `zdt_key` | `float` | `float` |  |  | ZDT_KEY |
| `zwo_key` | `float` | `float` |  |  | ZWO_KEY |
| `zor_key` | `float` | `float` |  |  | ZOR_KEY |
| `zmr_key` | `float` | `float` |  |  | ZMR_KEY |
| `zob_key` | `float` | `float` |  |  | ZOB_KEY |
| `zwt_orgcur` | `string` | `varchar` |  |  | ZWT_ORGCUR |
| `zwt_defcur` | `string` | `varchar` |  |  | ZWT_DEFCUR |
| `zwt_totalcostdefcur` | `float` | `float` |  |  | ZWT_TOTALCOSTDEFCUR |
| `zwt_ownlabcostdefcur` | `float` | `float` |  |  | ZWT_OWNLABCOSTDEFCUR |
| `zwt_ownlabcostorgcur` | `float` | `float` |  |  | ZWT_OWNLABCOSTORGCUR |
| `zwt_hiredlabcostdefcur` | `float` | `float` |  |  | ZWT_HIREDLABCOSTDEFCUR |
| `zwt_hiredlabcostorgcur` | `float` | `float` |  |  | ZWT_HIREDLABCOSTORGCUR |
| `zwt_fixedlabcostdefcur` | `float` | `float` |  |  | ZWT_FIXEDLABCOSTDEFCUR |
| `zwt_fixedlabcostorgcur` | `float` | `float` |  |  | ZWT_FIXEDLABCOSTORGCUR |
| `zwt_stockmatcostdefcur` | `float` | `float` |  |  | ZWT_STOCKMATCOSTDEFCUR |
| `zwt_stockmatcostorgcur` | `float` | `float` |  |  | ZWT_STOCKMATCOSTORGCUR |
| `zwt_dirmatcostdefcur` | `float` | `float` |  |  | ZWT_DIRMATCOSTDEFCUR |
| `zwt_dirmatcostorgcur` | `float` | `float` |  |  | ZWT_DIRMATCOSTORGCUR |
| `zwt_toolcostdefcur` | `float` | `float` |  |  | ZWT_TOOLCOSTDEFCUR |
| `zwt_toolcostorgcur` | `float` | `float` |  |  | ZWT_TOOLCOSTORGCUR |
| `zwt_wocode` | `string` | `varchar` |  |  | ZWT_WOCODE |
| `zwt_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ZWT_LASTSAVED |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

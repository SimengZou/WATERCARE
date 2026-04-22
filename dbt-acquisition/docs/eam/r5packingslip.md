# r5packingslip

eam_R5PACKINGSLIP

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5packingslip` |
| **Materialization** | `incremental` |
| **Primary keys** | `psl_dock`, `psl_line` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `psl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PSL_LASTSAVED |
| `psl_bin` | `string` | `varchar` |  |  | PSL_BIN |
| `psl_order` | `string` | `varchar` |  |  | PSL_ORDER |
| `psl_ordline` | `float` | `float` |  |  | PSL_ORDLINE |
| `psl_order_org` | `string` | `varchar` |  |  | PSL_ORDER_ORG |
| `psl_part` | `string` | `varchar` |  |  | PSL_PART |
| `psl_part_org` | `string` | `varchar` |  |  | PSL_PART_ORG |
| `psl_manlot` | `string` | `varchar` |  |  | PSL_MANLOT |
| `psl_manlotexp` | `timestamp` | `timestamp_ntz` |  |  | PSL_MANLOTEXP |
| `psl_supplierref` | `string` | `varchar` |  |  | PSL_SUPPLIERREF |
| `psl_deluom` | `string` | `varchar` |  |  | PSL_DELUOM |
| `psl_delqty` | `float` | `float` |  |  | PSL_DELQTY |
| `psl_recvqty` | `float` | `float` |  |  | PSL_RECVQTY |
| `psl_multiply` | `float` | `float` |  |  | PSL_MULTIPLY |
| `psl_comment` | `string` | `varchar` |  |  | PSL_COMMENT |
| `psl_updatecount` | `float` | `float` |  |  | PSL_UPDATECOUNT |
| `psl_dock` | `string` | `varchar` | `PK` |  | PSL_DOCK |
| `psl_line` | `float` | `float` | `PK` |  | PSL_LINE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5trackingctrl

eam_R5TRACKINGCTRL

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5trackingctrl` |
| **Materialization** | `incremental` |
| **Primary keys** | `tkc_interfacertype`, `tkc_uploadcolumn` |
| **Column count** | 16 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `tkc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | TKC_LASTSAVED |
| `tkc_interfacertype` | `string` | `varchar` | `PK` |  | TKC_INTERFACERTYPE |
| `tkc_column` | `string` | `varchar` |  |  | TKC_COLUMN |
| `tkc_rcolumn` | `string` | `varchar` |  |  | TKC_RCOLUMN |
| `tkc_displayseq` | `float` | `float` |  |  | TKC_DISPLAYSEQ |
| `tkc_length` | `float` | `float` |  |  | TKC_LENGTH |
| `tkc_datatype` | `string` | `varchar` |  |  | TKC_DATATYPE |
| `tkc_datartype` | `string` | `varchar` |  |  | TKC_DATARTYPE |
| `tkc_interfacetype` | `string` | `varchar` |  |  | TKC_INTERFACETYPE |
| `tkc_uploadcolumn` | `string` | `varchar` | `PK` |  | TKC_UPLOADCOLUMN |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

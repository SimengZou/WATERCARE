# r5manufacturers

eam_R5MANUFACTURERS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5manufacturers` |
| **Materialization** | `incremental` |
| **Primary keys** | `mfg_code` |
| **Column count** | 19 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `mfg_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | MFG_LASTSAVED |
| `mfg_desc` | `string` | `varchar` |  |  | MFG_DESC |
| `mfg_supplier` | `string` | `varchar` |  |  | MFG_SUPPLIER |
| `mfg_class` | `string` | `varchar` |  |  | MFG_CLASS |
| `mfg_sourcesystem` | `string` | `varchar` |  |  | MFG_SOURCESYSTEM |
| `mfg_sourcecode` | `string` | `varchar` |  |  | MFG_SOURCECODE |
| `mfg_class_org` | `string` | `varchar` |  |  | MFG_CLASS_ORG |
| `mfg_supplier_org` | `string` | `varchar` |  |  | MFG_SUPPLIER_ORG |
| `mfg_notused` | `string` | `varchar` |  |  | MFG_NOTUSED |
| `mfg_updatecount` | `float` | `float` |  |  | MFG_UPDATECOUNT |
| `mfg_updated` | `timestamp` | `timestamp_ntz` |  |  | MFG_UPDATED |
| `mfg_code` | `string` | `varchar` | `PK` | `unique` | MFG_CODE |
| `mfg_org` | `string` | `varchar` |  |  | MFG_ORG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

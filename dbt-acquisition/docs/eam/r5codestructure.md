# r5codestructure

eam_R5CODESTRUCTURE

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5codestructure` |
| **Materialization** | `incremental` |
| **Primary keys** | `csr_code` |
| **Column count** | 28 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `csr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | CSR_LASTSAVED |
| `csr_image` | `string` | `varchar` |  |  | CSR_IMAGE |
| `csr_rentity` | `string` | `varchar` |  |  | CSR_RENTITY |
| `csr_entity` | `string` | `varchar` |  |  | CSR_ENTITY |
| `csr_strlevel1` | `string` | `varchar` |  |  | CSR_STRLEVEL1 |
| `csr_strlevel2` | `string` | `varchar` |  |  | CSR_STRLEVEL2 |
| `csr_strlevel3` | `string` | `varchar` |  |  | CSR_STRLEVEL3 |
| `csr_strlevel4` | `string` | `varchar` |  |  | CSR_STRLEVEL4 |
| `csr_strlevel5` | `string` | `varchar` |  |  | CSR_STRLEVEL5 |
| `csr_strlevel6` | `string` | `varchar` |  |  | CSR_STRLEVEL6 |
| `csr_strlevel7` | `string` | `varchar` |  |  | CSR_STRLEVEL7 |
| `csr_strlevel8` | `string` | `varchar` |  |  | CSR_STRLEVEL8 |
| `csr_structure` | `string` | `varchar` |  |  | CSR_STRUCTURE |
| `csr_updatecount` | `float` | `float` |  |  | CSR_UPDATECOUNT |
| `csr_updated` | `timestamp` | `timestamp_ntz` |  |  | CSR_UPDATED |
| `csr_icon` | `string` | `varchar` |  |  | CSR_ICON |
| `csr_iconpath` | `string` | `varchar` |  |  | CSR_ICONPATH |
| `csr_importance` | `string` | `varchar` |  |  | CSR_IMPORTANCE |
| `csr_materialtype` | `string` | `varchar` |  |  | CSR_MATERIALTYPE |
| `csr_notused` | `string` | `varchar` |  |  | CSR_NOTUSED |
| `csr_code` | `string` | `varchar` | `PK` | `unique` | CSR_CODE |
| `csr_desc` | `string` | `varchar` |  |  | CSR_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

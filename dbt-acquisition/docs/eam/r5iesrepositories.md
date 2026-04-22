# r5iesrepositories

eam_R5IESREPOSITORIES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5iesrepositories` |
| **Materialization** | `incremental` |
| **Primary keys** | `ens_code` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ens_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ENS_LASTSAVED |
| `ens_dateupdatedcol` | `string` | `varchar` |  |  | ENS_DATEUPDATEDCOL |
| `ens_interestcenter` | `string` | `varchar` |  |  | ENS_INTERESTCENTER |
| `ens_notused` | `string` | `varchar` |  |  | ENS_NOTUSED |
| `ens_isincremental` | `string` | `varchar` |  |  | ENS_ISINCREMENTAL |
| `ens_ispopup` | `string` | `varchar` |  |  | ENS_ISPOPUP |
| `ens_datelastcrawl` | `timestamp` | `timestamp_ntz` |  |  | ENS_DATELASTCRAWL |
| `ens_updatecount` | `float` | `float` |  |  | ENS_UPDATECOUNT |
| `ens_code` | `string` | `varchar` | `PK` | `unique` | ENS_CODE |
| `ens_desc` | `string` | `varchar` |  |  | ENS_DESC |
| `ens_tablename` | `string` | `varchar` |  |  | ENS_TABLENAME |
| `ens_userfunction` | `string` | `varchar` |  |  | ENS_USERFUNCTION |
| `ens_tab` | `string` | `varchar` |  |  | ENS_TAB |
| `ens_thumbnail` | `string` | `varchar` |  |  | ENS_THUMBNAIL |
| `ens_datecreatedcol` | `string` | `varchar` |  |  | ENS_DATECREATEDCOL |
| `ens_orgcol` | `string` | `varchar` |  |  | ENS_ORGCOL |
| `ens_tableprefix` | `string` | `varchar` |  |  | ENS_TABLEPREFIX |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

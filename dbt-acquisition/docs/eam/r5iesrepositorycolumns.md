# r5iesrepositorycolumns

eam_R5IESREPOSITORYCOLUMNS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5iesrepositorycolumns` |
| **Materialization** | `incremental` |
| **Primary keys** | `enc_repositoryid`, `enc_columnname` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `enc_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ENC_LASTSAVED |
| `enc_columnname` | `string` | `varchar` | `PK` |  | ENC_COLUMNNAME |
| `enc_fieldname` | `string` | `varchar` |  |  | ENC_FIELDNAME |
| `enc_alias` | `string` | `varchar` |  |  | ENC_ALIAS |
| `enc_datatype` | `string` | `varchar` |  |  | ENC_DATATYPE |
| `enc_pkorder` | `float` | `float` |  |  | ENC_PKORDER |
| `enc_isjspkeyfield` | `string` | `varchar` |  |  | ENC_ISJSPKEYFIELD |
| `enc_facet` | `string` | `varchar` |  |  | ENC_FACET |
| `enc_updatecount` | `float` | `float` |  |  | ENC_UPDATECOUNT |
| `enc_hiddenfromsearchresults` | `string` | `varchar` |  |  | ENC_HIDDENFROMSEARCHRESULTS |
| `enc_repositoryid` | `string` | `varchar` | `PK` |  | ENC_REPOSITORYID |
| `enc_displayorder` | `float` | `float` |  |  | ENC_DISPLAYORDER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

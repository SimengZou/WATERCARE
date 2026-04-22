# r5hyperlink

eam_R5HYPERLINK

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5hyperlink` |
| **Materialization** | `incremental` |
| **Primary keys** | `hyp_pk` |
| **Column count** | 18 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `hyp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | HYP_LASTSAVED |
| `hyp_sourceelementid` | `string` | `varchar` |  |  | HYP_SOURCEELEMENTID |
| `hyp_destpagename` | `string` | `varchar` |  |  | HYP_DESTPAGENAME |
| `hyp_destelementid` | `string` | `varchar` |  |  | HYP_DESTELEMENTID |
| `hyp_screenmode` | `string` | `varchar` |  |  | HYP_SCREENMODE |
| `hyp_performexactquery` | `string` | `varchar` |  |  | HYP_PERFORMEXACTQUERY |
| `hyp_updatecount` | `float` | `float` |  |  | HYP_UPDATECOUNT |
| `hyp_srclinenumber` | `float` | `float` |  |  | HYP_SRCLINENUMBER |
| `hyp_linkname` | `string` | `varchar` |  |  | HYP_LINKNAME |
| `hyp_pk` | `float` | `float` | `PK` | `unique` | HYP_PK |
| `hyp_sourcepagename` | `string` | `varchar` |  |  | HYP_SOURCEPAGENAME |
| `hyp_dataspy` | `float` | `float` |  |  | HYP_DATASPY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5revcntrlsetup

eam_R5REVCNTRLSETUP

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5revcntrlsetup` |
| **Materialization** | `incremental` |
| **Primary keys** | `rcs_pagename`, `rcs_elementid` |
| **Column count** | 12 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `rcs_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | RCS_LASTSAVED |
| `rcs_elementid` | `string` | `varchar` | `PK` |  | RCS_ELEMENTID |
| `rcs_protected` | `string` | `varchar` |  |  | RCS_PROTECTED |
| `rcs_updatecount` | `float` | `float` |  |  | RCS_UPDATECOUNT |
| `rcs_pagename` | `string` | `varchar` | `PK` |  | RCS_PAGENAME |
| `rcs_xpath` | `string` | `varchar` |  |  | RCS_XPATH |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5pagelayout_bak

eam_R5PAGELAYOUT_BAK

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5pagelayout_bak` |
| **Materialization** | `incremental` |
| **Column count** | 25 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `plo_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PLO_LASTSAVED |
| `plo_usergroup` | `string` | `varchar` |  |  | PLO_USERGROUP |
| `plo_elementtype` | `string` | `varchar` |  |  | PLO_ELEMENTTYPE |
| `plo_presentinjsp` | `string` | `varchar` |  |  | PLO_PRESENTINJSP |
| `plo_attribute` | `string` | `varchar` |  |  | PLO_ATTRIBUTE |
| `plo_fieldcontainer` | `string` | `varchar` |  |  | PLO_FIELDCONTAINER |
| `plo_fieldgroup` | `float` | `float` |  |  | PLO_FIELDGROUP |
| `plo_positioningroup` | `float` | `float` |  |  | PLO_POSITIONINGROUP |
| `plo_fieldconttype` | `float` | `float` |  |  | PLO_FIELDCONTTYPE |
| `plo_tabindex` | `float` | `float` |  |  | PLO_TABINDEX |
| `plo_systemattribute` | `string` | `varchar` |  |  | PLO_SYSTEMATTRIBUTE |
| `plo_changed` | `string` | `varchar` |  |  | PLO_CHANGED |
| `plo_updatecount` | `float` | `float` |  |  | PLO_UPDATECOUNT |
| `plo_mekey` | `string` | `varchar` |  |  | PLO_MEKEY |
| `plo_meflag` | `string` | `varchar` |  |  | PLO_MEFLAG |
| `plo_defaultvalue` | `string` | `varchar` |  |  | PLO_DEFAULTVALUE |
| `plo_parentpage` | `string` | `varchar` |  |  | PLO_PARENTPAGE |
| `plo_pagename` | `string` | `varchar` |  |  | PLO_PAGENAME |
| `plo_elementid` | `string` | `varchar` |  |  | PLO_ELEMENTID |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

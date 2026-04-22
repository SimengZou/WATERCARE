# r5duxlayout

eam_R5DUXLAYOUT

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5duxlayout` |
| **Materialization** | `incremental` |
| **Primary keys** | `dxl_usergroup`, `dxl_pagename`, `dxl_elementid` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `dxl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | DXL_LASTSAVED |
| `dxl_elementid` | `string` | `varchar` | `PK` |  | DXL_ELEMENTID |
| `dxl_attribute` | `string` | `varchar` |  |  | DXL_ATTRIBUTE |
| `dxl_systemattribute` | `string` | `varchar` |  |  | DXL_SYSTEMATTRIBUTE |
| `dxl_presentinjsp` | `string` | `varchar` |  |  | DXL_PRESENTINJSP |
| `dxl_section` | `string` | `varchar` |  |  | DXL_SECTION |
| `dxl_sectionposition` | `float` | `float` |  |  | DXL_SECTIONPOSITION |
| `dxl_positioningroup` | `float` | `float` |  |  | DXL_POSITIONINGROUP |
| `dxl_sectionlabel` | `string` | `varchar` |  |  | DXL_SECTIONLABEL |
| `dxl_newcard` | `string` | `varchar` |  |  | DXL_NEWCARD |
| `dxl_fieldinfo` | `string` | `varchar` |  |  | DXL_FIELDINFO |
| `dxl_defaultvalue` | `string` | `varchar` |  |  | DXL_DEFAULTVALUE |
| `dxl_radiooptions` | `string` | `varchar` |  |  | DXL_RADIOOPTIONS |
| `dxl_updatecount` | `float` | `float` |  |  | DXL_UPDATECOUNT |
| `dxl_usergroup` | `string` | `varchar` | `PK` |  | DXL_USERGROUP |
| `dxl_pagename` | `string` | `varchar` | `PK` |  | DXL_PAGENAME |
| `dxl_elementtype` | `string` | `varchar` |  |  | DXL_ELEMENTTYPE |
| `dxl_fieldsize` | `float` | `float` |  |  | DXL_FIELDSIZE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

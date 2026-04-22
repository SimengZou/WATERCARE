# r5gridfield

eam_R5GRIDFIELD

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5gridfield` |
| **Materialization** | `incremental` |
| **Primary keys** | `gfd_fieldid`, `gfd_gridid`, `gfd_occurrence` |
| **Column count** | 23 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `gfd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | GFD_LASTSAVED |
| `gfd_botfunction` | `string` | `varchar` |  |  | GFD_BOTFUNCTION |
| `gfd_botnumber` | `float` | `float` |  |  | GFD_BOTNUMBER |
| `gfd_controltype` | `string` | `varchar` |  |  | GFD_CONTROLTYPE |
| `gfd_scriptevent` | `string` | `varchar` |  |  | GFD_SCRIPTEVENT |
| `gfd_tagname` | `string` | `varchar` |  |  | GFD_TAGNAME |
| `gfd_updatecount` | `float` | `float` |  |  | GFD_UPDATECOUNT |
| `gfd_tagparams` | `string` | `varchar` |  |  | GFD_TAGPARAMS |
| `gfd_aggfunc` | `string` | `varchar` |  |  | GFD_AGGFUNC |
| `gfd_fieldtype` | `string` | `varchar` |  |  | GFD_FIELDTYPE |
| `gfd_occurrence` | `float` | `float` | `PK` |  | GFD_OCCURRENCE |
| `gfd_dependent` | `float` | `float` |  |  | GFD_DEPENDENT |
| `gfd_secentity` | `string` | `varchar` |  |  | GFD_SECENTITY |
| `gfd_headerlocation` | `string` | `varchar` |  |  | GFD_HEADERLOCATION |
| `gfd_updated` | `timestamp` | `timestamp_ntz` |  |  | GFD_UPDATED |
| `gfd_fieldid` | `float` | `float` | `PK` |  | GFD_FIELDID |
| `gfd_gridid` | `float` | `float` | `PK` |  | GFD_GRIDID |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

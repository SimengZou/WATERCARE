# r5grid

eam_R5GRID

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5grid` |
| **Materialization** | `incremental` |
| **Primary keys** | `grd_gridid` |
| **Column count** | 41 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `grd_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | GRD_LASTSAVED |
| `grd_maxgridcost` | `float` | `float` |  |  | GRD_MAXGRIDCOST |
| `grd_basequery` | `string` | `varchar` |  |  | GRD_BASEQUERY |
| `grd_gridname` | `string` | `varchar` |  |  | GRD_GRIDNAME |
| `grd_keyfields` | `string` | `varchar` |  |  | GRD_KEYFIELDS |
| `grd_filterablelist` | `string` | `varchar` |  |  | GRD_FILTERABLELIST |
| `grd_sortablelist` | `string` | `varchar` |  |  | GRD_SORTABLELIST |
| `grd_displayablelist` | `string` | `varchar` |  |  | GRD_DISPLAYABLELIST |
| `grd_orgcolname` | `string` | `varchar` |  |  | GRD_ORGCOLNAME |
| `grd_basequery_multiorg` | `string` | `varchar` |  |  | GRD_BASEQUERY_MULTIORG |
| `grd_keyfields_multiorg` | `string` | `varchar` |  |  | GRD_KEYFIELDS_MULTIORG |
| `grd_filterable_multiorg` | `string` | `varchar` |  |  | GRD_FILTERABLE_MULTIORG |
| `grd_sortable_multiorg` | `string` | `varchar` |  |  | GRD_SORTABLE_MULTIORG |
| `grd_displayable_multiorg` | `string` | `varchar` |  |  | GRD_DISPLAYABLE_MULTIORG |
| `grd_botfunction` | `string` | `varchar` |  |  | GRD_BOTFUNCTION |
| `grd_updatecount` | `float` | `float` |  |  | GRD_UPDATECOUNT |
| `grd_portletflag` | `string` | `varchar` |  |  | GRD_PORTLETFLAG |
| `grd_secentity` | `string` | `varchar` |  |  | GRD_SECENTITY |
| `grd_hints` | `string` | `varchar` |  |  | GRD_HINTS |
| `grd_tab` | `string` | `varchar` |  |  | GRD_TAB |
| `grd_gridtype` | `float` | `float` |  |  | GRD_GRIDTYPE |
| `grd_units` | `string` | `varchar` |  |  | GRD_UNITS |
| `grd_optimizeron` | `string` | `varchar` |  |  | GRD_OPTIMIZERON |
| `grd_distinct` | `string` | `varchar` |  |  | GRD_DISTINCT |
| `grd_commitflag` | `string` | `varchar` |  |  | GRD_COMMITFLAG |
| `grd_customfieldcode` | `string` | `varchar` |  |  | GRD_CUSTOMFIELDCODE |
| `grd_complex` | `string` | `varchar` |  |  | GRD_COMPLEX |
| `grd_active` | `string` | `varchar` |  |  | GRD_ACTIVE |
| `grd_noscreendesigner` | `string` | `varchar` |  |  | GRD_NOSCREENDESIGNER |
| `grd_mobile` | `string` | `varchar` |  |  | GRD_MOBILE |
| `grd_updated` | `timestamp` | `timestamp_ntz` |  |  | GRD_UPDATED |
| `grd_gisdatafilter` | `string` | `varchar` |  |  | GRD_GISDATAFILTER |
| `grd_giswofieldmap` | `string` | `varchar` |  |  | GRD_GISWOFIELDMAP |
| `grd_desc` | `string` | `varchar` |  |  | GRD_DESC |
| `grd_gridid` | `float` | `float` | `PK` | `unique` | GRD_GRIDID |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

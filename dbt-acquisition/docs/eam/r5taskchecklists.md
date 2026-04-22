# r5taskchecklists

eam_R5TASKCHECKLISTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5taskchecklists` |
| **Materialization** | `incremental` |
| **Primary keys** | `tch_code` |
| **Column count** | 61 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `tch_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | TCH_LASTSAVED |
| `tch_metricfractionslider` | `string` | `varchar` |  |  | TCH_METRICFRACTIONSLIDER |
| `tch_taskrev` | `float` | `float` |  |  | TCH_TASKREV |
| `tch_desc` | `string` | `varchar` |  |  | TCH_DESC |
| `tch_sequence` | `float` | `float` |  |  | TCH_SEQUENCE |
| `tch_notused` | `string` | `varchar` |  |  | TCH_NOTUSED |
| `tch_updated` | `timestamp` | `timestamp_ntz` |  |  | TCH_UPDATED |
| `tch_updatedby` | `string` | `varchar` |  |  | TCH_UPDATEDBY |
| `tch_updatecount` | `float` | `float` |  |  | TCH_UPDATECOUNT |
| `tch_type` | `string` | `varchar` |  |  | TCH_TYPE |
| `tch_requiredtoclose` | `string` | `varchar` |  |  | TCH_REQUIREDTOCLOSE |
| `tch_objectlevel` | `string` | `varchar` |  |  | TCH_OBJECTLEVEL |
| `tch_objectclass` | `string` | `varchar` |  |  | TCH_OBJECTCLASS |
| `tch_objectclass_org` | `string` | `varchar` |  |  | TCH_OBJECTCLASS_ORG |
| `tch_objectcategory` | `string` | `varchar` |  |  | TCH_OBJECTCATEGORY |
| `tch_uom` | `string` | `varchar` |  |  | TCH_UOM |
| `tch_aspect` | `string` | `varchar` |  |  | TCH_ASPECT |
| `tch_pointtype` | `string` | `varchar` |  |  | TCH_POINTTYPE |
| `tch_repeating` | `string` | `varchar` |  |  | TCH_REPEATING |
| `tch_followuptask` | `string` | `varchar` |  |  | TCH_FOLLOWUPTASK |
| `tch_matlist` | `string` | `varchar` |  |  | TCH_MATLIST |
| `tch_syslevel` | `string` | `varchar` |  |  | TCH_SYSLEVEL |
| `tch_asmlevel` | `string` | `varchar` |  |  | TCH_ASMLEVEL |
| `tch_complevel` | `string` | `varchar` |  |  | TCH_COMPLEVEL |
| `tch_complocation` | `string` | `varchar` |  |  | TCH_COMPLOCATION |
| `tch_condition` | `string` | `varchar` |  |  | TCH_CONDITION |
| `tch_possiblefindings` | `string` | `varchar` |  |  | TCH_POSSIBLEFINDINGS |
| `tch_followupjob` | `string` | `varchar` |  |  | TCH_FOLLOWUPJOB |
| `tch_part` | `string` | `varchar` |  |  | TCH_PART |
| `tch_part_org` | `string` | `varchar` |  |  | TCH_PART_ORG |
| `tch_partcondition` | `string` | `varchar` |  |  | TCH_PARTCONDITION |
| `tch_partclass` | `string` | `varchar` |  |  | TCH_PARTCLASS |
| `tch_partclass_org` | `string` | `varchar` |  |  | TCH_PARTCLASS_ORG |
| `tch_partcategory` | `string` | `varchar` |  |  | TCH_PARTCATEGORY |
| `tch_color` | `string` | `varchar` |  |  | TCH_COLOR |
| `tch_reference` | `string` | `varchar` |  |  | TCH_REFERENCE |
| `tch_equipfilter` | `string` | `varchar` |  |  | TCH_EQUIPFILTER |
| `tch_minslidervalue` | `float` | `float` |  |  | TCH_MINSLIDERVALUE |
| `tch_maxslidervalue` | `float` | `float` |  |  | TCH_MAXSLIDERVALUE |
| `tch_srccalculatedvalue` | `string` | `varchar` |  |  | TCH_SRCCALCULATEDVALUE |
| `tch_formula` | `string` | `varchar` |  |  | TCH_FORMULA |
| `tch_direction_options` | `string` | `varchar` |  |  | TCH_DIRECTION_OPTIONS |
| `tch_not_applicable_options` | `string` | `varchar` |  |  | TCH_NOT_APPLICABLE_OPTIONS |
| `tch_condition_options` | `string` | `varchar` |  |  | TCH_CONDITION_OPTIONS |
| `tch_group_label` | `string` | `varchar` |  |  | TCH_GROUP_LABEL |
| `tch_responsibility` | `string` | `varchar` |  |  | TCH_RESPONSIBILITY |
| `tch_measurementresp` | `string` | `varchar` |  |  | TCH_MEASUREMENTRESP |
| `tch_hidefollowup` | `string` | `varchar` |  |  | TCH_HIDEFOLLOWUP |
| `tch_hidelinearfields` | `string` | `varchar` |  |  | TCH_HIDELINEARFIELDS |
| `tch_enablenonconformities` | `string` | `varchar` |  |  | TCH_ENABLENONCONFORMITIES |
| `tch_fractionsliderdimensions` | `string` | `varchar` |  |  | TCH_FRACTIONSLIDERDIMENSIONS |
| `tch_resultrentity` | `string` | `varchar` |  |  | TCH_RESULTRENTITY |
| `tch_resultentityclassoptions` | `string` | `varchar` |  |  | TCH_RESULTENTITYCLASSOPTIONS |
| `tch_code` | `string` | `varchar` | `PK` | `unique` | TCH_CODE |
| `tch_task` | `string` | `varchar` |  |  | TCH_TASK |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

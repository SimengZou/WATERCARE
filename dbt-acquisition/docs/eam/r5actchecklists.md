# r5actchecklists

eam_R5ACTCHECKLISTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5actchecklists` |
| **Materialization** | `incremental` |
| **Primary keys** | `ack_code` |
| **Column count** | 141 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ack_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ACK_LASTSAVED |
| `ack_code` | `string` | `varchar` | `PK` | `unique` | ACK_CODE |
| `ack_event` | `string` | `varchar` |  |  | ACK_EVENT |
| `ack_act` | `float` | `float` |  |  | ACK_ACT |
| `ack_desc` | `string` | `varchar` |  |  | ACK_DESC |
| `ack_sequence` | `float` | `float` |  |  | ACK_SEQUENCE |
| `ack_completed` | `string` | `varchar` |  |  | ACK_COMPLETED |
| `ack_notes` | `string` | `varchar` |  |  | ACK_NOTES |
| `ack_updated` | `timestamp` | `timestamp_ntz` |  |  | ACK_UPDATED |
| `ack_updatedby` | `string` | `varchar` |  |  | ACK_UPDATEDBY |
| `ack_updatecount` | `float` | `float` |  |  | ACK_UPDATECOUNT |
| `ack_loto` | `string` | `varchar` |  |  | ACK_LOTO |
| `ack_occurrence` | `float` | `float` |  |  | ACK_OCCURRENCE |
| `ack_point` | `string` | `varchar` |  |  | ACK_POINT |
| `ack_object` | `string` | `varchar` |  |  | ACK_OBJECT |
| `ack_object_org` | `string` | `varchar` |  |  | ACK_OBJECT_ORG |
| `ack_yes` | `string` | `varchar` |  |  | ACK_YES |
| `ack_no` | `string` | `varchar` |  |  | ACK_NO |
| `ack_finding` | `string` | `varchar` |  |  | ACK_FINDING |
| `ack_value` | `float` | `float` |  |  | ACK_VALUE |
| `ack_uom` | `string` | `varchar` |  |  | ACK_UOM |
| `ack_finaloccurrence` | `string` | `varchar` |  |  | ACK_FINALOCCURRENCE |
| `ack_followup` | `string` | `varchar` |  |  | ACK_FOLLOWUP |
| `ack_followupevent` | `string` | `varchar` |  |  | ACK_FOLLOWUPEVENT |
| `ack_followupact` | `float` | `float` |  |  | ACK_FOLLOWUPACT |
| `ack_requiredtoclose` | `string` | `varchar` |  |  | ACK_REQUIREDTOCLOSE |
| `ack_syslevel` | `string` | `varchar` |  |  | ACK_SYSLEVEL |
| `ack_asmlevel` | `string` | `varchar` |  |  | ACK_ASMLEVEL |
| `ack_complevel` | `string` | `varchar` |  |  | ACK_COMPLEVEL |
| `ack_complocation` | `string` | `varchar` |  |  | ACK_COMPLOCATION |
| `ack_condition` | `string` | `varchar` |  |  | ACK_CONDITION |
| `ack_reading` | `string` | `varchar` |  |  | ACK_READING |
| `ack_aspect` | `string` | `varchar` |  |  | ACK_ASPECT |
| `ack_pointtype` | `string` | `varchar` |  |  | ACK_POINTTYPE |
| `ack_task` | `string` | `varchar` |  |  | ACK_TASK |
| `ack_matlist` | `string` | `varchar` |  |  | ACK_MATLIST |
| `ack_type` | `string` | `varchar` |  |  | ACK_TYPE |
| `ack_parentkey` | `string` | `varchar` |  |  | ACK_PARENTKEY |
| `ack_possiblefindings` | `string` | `varchar` |  |  | ACK_POSSIBLEFINDINGS |
| `ack_taskchecklistcode` | `string` | `varchar` |  |  | ACK_TASKCHECKLISTCODE |
| `ack_objectlevel` | `string` | `varchar` |  |  | ACK_OBJECTLEVEL |
| `ack_defmaint` | `string` | `varchar` |  |  | ACK_DEFMAINT |
| `ack_part` | `string` | `varchar` |  |  | ACK_PART |
| `ack_part_org` | `string` | `varchar` |  |  | ACK_PART_ORG |
| `ack_partcondition` | `string` | `varchar` |  |  | ACK_PARTCONDITION |
| `ack_frompoint` | `float` | `float` |  |  | ACK_FROMPOINT |
| `ack_fromrefdesc` | `string` | `varchar` |  |  | ACK_FROMREFDESC |
| `ack_fromgeoref` | `string` | `varchar` |  |  | ACK_FROMGEOREF |
| `ack_topoint` | `float` | `float` |  |  | ACK_TOPOINT |
| `ack_torefdesc` | `string` | `varchar` |  |  | ACK_TOREFDESC |
| `ack_togeoref` | `string` | `varchar` |  |  | ACK_TOGEOREF |
| `ack_fromjobplan` | `string` | `varchar` |  |  | ACK_FROMJOBPLAN |
| `ack_fromjobplanrev` | `float` | `float` |  |  | ACK_FROMJOBPLANREV |
| `ack_rentity` | `string` | `varchar` |  |  | ACK_RENTITY |
| `ack_entitykey` | `string` | `varchar` |  |  | ACK_ENTITYKEY |
| `ack_entityorg` | `string` | `varchar` |  |  | ACK_ENTITYORG |
| `ack_routesequence` | `float` | `float` |  |  | ACK_ROUTESEQUENCE |
| `ack_job` | `string` | `varchar` |  |  | ACK_JOB |
| `ack_casetask` | `string` | `varchar` |  |  | ACK_CASETASK |
| `ack_nonconformity` | `string` | `varchar` |  |  | ACK_NONCONFORMITY |
| `ack_from_reference` | `float` | `float` |  |  | ACK_FROM_REFERENCE |
| `ack_from_offset` | `float` | `float` |  |  | ACK_FROM_OFFSET |
| `ack_from_offset_percentage` | `float` | `float` |  |  | ACK_FROM_OFFSET_PERCENTAGE |
| `ack_from_offset_direction` | `string` | `varchar` |  |  | ACK_FROM_OFFSET_DIRECTION |
| `ack_from_xcoordinate` | `float` | `float` |  |  | ACK_FROM_XCOORDINATE |
| `ack_from_ycoordinate` | `float` | `float` |  |  | ACK_FROM_YCOORDINATE |
| `ack_from_latitude` | `float` | `float` |  |  | ACK_FROM_LATITUDE |
| `ack_from_longitude` | `float` | `float` |  |  | ACK_FROM_LONGITUDE |
| `ack_from_relationship_type` | `string` | `varchar` |  |  | ACK_FROM_RELATIONSHIP_TYPE |
| `ack_from_horizontal_offset` | `float` | `float` |  |  | ACK_FROM_HORIZONTAL_OFFSET |
| `ack_from_horizontal_offsetuom` | `string` | `varchar` |  |  | ACK_FROM_HORIZONTAL_OFFSETUOM |
| `ack_from_horizontal_offsettype` | `string` | `varchar` |  |  | ACK_FROM_HORIZONTAL_OFFSETTYPE |
| `ack_from_vertical_offset` | `float` | `float` |  |  | ACK_FROM_VERTICAL_OFFSET |
| `ack_from_vertical_offsetuom` | `string` | `varchar` |  |  | ACK_FROM_VERTICAL_OFFSETUOM |
| `ack_from_vertical_offsettype` | `string` | `varchar` |  |  | ACK_FROM_VERTICAL_OFFSETTYPE |
| `ack_to_reference` | `float` | `float` |  |  | ACK_TO_REFERENCE |
| `ack_to_offset` | `float` | `float` |  |  | ACK_TO_OFFSET |
| `ack_to_offset_direction` | `string` | `varchar` |  |  | ACK_TO_OFFSET_DIRECTION |
| `ack_to_offset_percentage` | `float` | `float` |  |  | ACK_TO_OFFSET_PERCENTAGE |
| `ack_to_xcoordinate` | `float` | `float` |  |  | ACK_TO_XCOORDINATE |
| `ack_to_ycoordinate` | `float` | `float` |  |  | ACK_TO_YCOORDINATE |
| `ack_to_latitude` | `float` | `float` |  |  | ACK_TO_LATITUDE |
| `ack_to_longitude` | `float` | `float` |  |  | ACK_TO_LONGITUDE |
| `ack_to_relationship_type` | `string` | `varchar` |  |  | ACK_TO_RELATIONSHIP_TYPE |
| `ack_to_horizontal_offset` | `float` | `float` |  |  | ACK_TO_HORIZONTAL_OFFSET |
| `ack_to_horizontal_offsetuom` | `string` | `varchar` |  |  | ACK_TO_HORIZONTAL_OFFSETUOM |
| `ack_to_horizontal_offsettype` | `string` | `varchar` |  |  | ACK_TO_HORIZONTAL_OFFSETTYPE |
| `ack_to_vertical_offset` | `float` | `float` |  |  | ACK_TO_VERTICAL_OFFSET |
| `ack_to_vertical_offsetuom` | `string` | `varchar` |  |  | ACK_TO_VERTICAL_OFFSETUOM |
| `ack_to_vertical_offsettype` | `string` | `varchar` |  |  | ACK_TO_VERTICAL_OFFSETTYPE |
| `ack_relationship_type` | `string` | `varchar` |  |  | ACK_RELATIONSHIP_TYPE |
| `ack_color` | `string` | `varchar` |  |  | ACK_COLOR |
| `ack_reference` | `string` | `varchar` |  |  | ACK_REFERENCE |
| `ack_ok` | `string` | `varchar` |  |  | ACK_OK |
| `ack_repairsneeded` | `string` | `varchar` |  |  | ACK_REPAIRSNEEDED |
| `ack_resolution` | `string` | `varchar` |  |  | ACK_RESOLUTION |
| `ack_good` | `string` | `varchar` |  |  | ACK_GOOD |
| `ack_poor` | `string` | `varchar` |  |  | ACK_POOR |
| `ack_adjusted` | `string` | `varchar` |  |  | ACK_ADJUSTED |
| `ack_nonconformityfound` | `string` | `varchar` |  |  | ACK_NONCONFORMITYFOUND |
| `ack_minslidervalue` | `float` | `float` |  |  | ACK_MINSLIDERVALUE |
| `ack_maxslidervalue` | `float` | `float` |  |  | ACK_MAXSLIDERVALUE |
| `ack_srccalculatedvalue` | `string` | `varchar` |  |  | ACK_SRCCALCULATEDVALUE |
| `ack_formula` | `string` | `varchar` |  |  | ACK_FORMULA |
| `ack_possible_directions` | `string` | `varchar` |  |  | ACK_POSSIBLE_DIRECTIONS |
| `ack_possible_na_options` | `string` | `varchar` |  |  | ACK_POSSIBLE_NA_OPTIONS |
| `ack_possible_conds_as_found` | `string` | `varchar` |  |  | ACK_POSSIBLE_CONDS_AS_FOUND |
| `ack_possible_severities` | `string` | `varchar` |  |  | ACK_POSSIBLE_SEVERITIES |
| `ack_group_label` | `string` | `varchar` |  |  | ACK_GROUP_LABEL |
| `ack_direction` | `string` | `varchar` |  |  | ACK_DIRECTION |
| `ack_not_applicable` | `string` | `varchar` |  |  | ACK_NOT_APPLICABLE |
| `ack_condition_as_found` | `string` | `varchar` |  |  | ACK_CONDITION_AS_FOUND |
| `ack_severity` | `string` | `varchar` |  |  | ACK_SEVERITY |
| `ack_nonconformitytype` | `string` | `varchar` |  |  | ACK_NONCONFORMITYTYPE |
| `ack_nonconformitytype_org` | `string` | `varchar` |  |  | ACK_NONCONFORMITYTYPE_ORG |
| `ack_toleranceseverity` | `string` | `varchar` |  |  | ACK_TOLERANCESEVERITY |
| `ack_tolerance_ncf_type` | `string` | `varchar` |  |  | ACK_TOLERANCE_NCF_TYPE |
| `ack_tolerance_ncf_type_org` | `string` | `varchar` |  |  | ACK_TOLERANCE_NCF_TYPE_ORG |
| `ack_tolerancecrnonconformity` | `string` | `varchar` |  |  | ACK_TOLERANCECRNONCONFORMITY |
| `ack_metricfractionslider` | `string` | `varchar` |  |  | ACK_METRICFRACTIONSLIDER |
| `ack_responsibility` | `string` | `varchar` |  |  | ACK_RESPONSIBILITY |
| `ack_measurementresp` | `string` | `varchar` |  |  | ACK_MEASUREMENTRESP |
| `ack_hidefollowup` | `string` | `varchar` |  |  | ACK_HIDEFOLLOWUP |
| `ack_hidelinearfields` | `string` | `varchar` |  |  | ACK_HIDELINEARFIELDS |
| `ack_enablenonconformities` | `string` | `varchar` |  |  | ACK_ENABLENONCONFORMITIES |
| `ack_mobiledateupdated` | `timestamp` | `timestamp_ntz` |  |  | ACK_MOBILEDATEUPDATED |
| `ack_linearlocation` | `float` | `float` |  |  | ACK_LINEARLOCATION |
| `ack_fractionsliderdimensions` | `string` | `varchar` |  |  | ACK_FRACTIONSLIDERDIMENSIONS |
| `ack_checklistdatetime` | `timestamp` | `timestamp_ntz` |  |  | ACK_CHECKLISTDATETIME |
| `ack_checklistdate` | `timestamp` | `timestamp_ntz` |  |  | ACK_CHECKLISTDATE |
| `ack_freetext` | `string` | `varchar` |  |  | ACK_FREETEXT |
| `ack_resultrentity` | `string` | `varchar` |  |  | ACK_RESULTRENTITY |
| `ack_resultentityclassoptions` | `string` | `varchar` |  |  | ACK_RESULTENTITYCLASSOPTIONS |
| `ack_resultentitycode` | `string` | `varchar` |  |  | ACK_RESULTENTITYCODE |
| `ack_resultentityorg` | `string` | `varchar` |  |  | ACK_RESULTENTITYORG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

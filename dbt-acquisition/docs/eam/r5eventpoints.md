# r5eventpoints

eam_R5EVENTPOINTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5eventpoints` |
| **Materialization** | `incremental` |
| **Primary keys** | `epo_code` |
| **Column count** | 182 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `epo_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | EPO_LASTSAVED |
| `epo_event` | `string` | `varchar` |  |  | EPO_EVENT |
| `epo_line` | `float` | `float` |  |  | EPO_LINE |
| `epo_object` | `string` | `varchar` |  |  | EPO_OBJECT |
| `epo_obrtype` | `string` | `varchar` |  |  | EPO_OBRTYPE |
| `epo_obtype` | `string` | `varchar` |  |  | EPO_OBTYPE |
| `epo_point` | `string` | `varchar` |  |  | EPO_POINT |
| `epo_pointtype` | `string` | `varchar` |  |  | EPO_POINTTYPE |
| `epo_aspect` | `string` | `varchar` |  |  | EPO_ASPECT |
| `epo_avgslope` | `float` | `float` |  |  | EPO_AVGSLOPE |
| `epo_comment` | `string` | `varchar` |  |  | EPO_COMMENT |
| `epo_create` | `string` | `varchar` |  |  | EPO_CREATE |
| `epo_date` | `timestamp` | `timestamp_ntz` |  |  | EPO_DATE |
| `epo_finding` | `string` | `varchar` |  |  | EPO_FINDING |
| `epo_job` | `string` | `varchar` |  |  | EPO_JOB |
| `epo_lastslope` | `float` | `float` |  |  | EPO_LASTSLOPE |
| `epo_max` | `float` | `float` |  |  | EPO_MAX |
| `epo_varchar25` | `string` | `varchar` |  |  | EPO_VARCHAR25 |
| `epo_varchar26` | `string` | `varchar` |  |  | EPO_VARCHAR26 |
| `epo_varchar27` | `string` | `varchar` |  |  | EPO_VARCHAR27 |
| `epo_varchar28` | `string` | `varchar` |  |  | EPO_VARCHAR28 |
| `epo_varchar29` | `string` | `varchar` |  |  | EPO_VARCHAR29 |
| `epo_varchar30` | `string` | `varchar` |  |  | EPO_VARCHAR30 |
| `epo_varchar31` | `string` | `varchar` |  |  | EPO_VARCHAR31 |
| `epo_varchar32` | `string` | `varchar` |  |  | EPO_VARCHAR32 |
| `epo_varchar33` | `string` | `varchar` |  |  | EPO_VARCHAR33 |
| `epo_varchar34` | `string` | `varchar` |  |  | EPO_VARCHAR34 |
| `epo_varchar35` | `string` | `varchar` |  |  | EPO_VARCHAR35 |
| `epo_varnum11` | `float` | `float` |  |  | EPO_VARNUM11 |
| `epo_varnum12` | `float` | `float` |  |  | EPO_VARNUM12 |
| `epo_varnum13` | `float` | `float` |  |  | EPO_VARNUM13 |
| `epo_varnum14` | `float` | `float` |  |  | EPO_VARNUM14 |
| `epo_varnum15` | `float` | `float` |  |  | EPO_VARNUM15 |
| `epo_varnum16` | `float` | `float` |  |  | EPO_VARNUM16 |
| `epo_varnum17` | `float` | `float` |  |  | EPO_VARNUM17 |
| `epo_varnum18` | `float` | `float` |  |  | EPO_VARNUM18 |
| `epo_varnum19` | `float` | `float` |  |  | EPO_VARNUM19 |
| `epo_varnum20` | `float` | `float` |  |  | EPO_VARNUM20 |
| `epo_varnum21` | `float` | `float` |  |  | EPO_VARNUM21 |
| `epo_varnum22` | `float` | `float` |  |  | EPO_VARNUM22 |
| `epo_varnum23` | `float` | `float` |  |  | EPO_VARNUM23 |
| `epo_varnum24` | `float` | `float` |  |  | EPO_VARNUM24 |
| `epo_varnum25` | `float` | `float` |  |  | EPO_VARNUM25 |
| `epo_varnum26` | `float` | `float` |  |  | EPO_VARNUM26 |
| `epo_varnum27` | `float` | `float` |  |  | EPO_VARNUM27 |
| `epo_varnum28` | `float` | `float` |  |  | EPO_VARNUM28 |
| `epo_varnum29` | `float` | `float` |  |  | EPO_VARNUM29 |
| `epo_varnum30` | `float` | `float` |  |  | EPO_VARNUM30 |
| `epo_varnum31` | `float` | `float` |  |  | EPO_VARNUM31 |
| `epo_varnum32` | `float` | `float` |  |  | EPO_VARNUM32 |
| `epo_maxdate` | `timestamp` | `timestamp_ntz` |  |  | EPO_MAXDATE |
| `epo_maxppm` | `string` | `varchar` |  |  | EPO_MAXPPM |
| `epo_maxstwo` | `string` | `varchar` |  |  | EPO_MAXSTWO |
| `epo_maxtol` | `float` | `float` |  |  | EPO_MAXTOL |
| `epo_maxtoldate` | `timestamp` | `timestamp_ntz` |  |  | EPO_MAXTOLDATE |
| `epo_method` | `string` | `varchar` |  |  | EPO_METHOD |
| `epo_min` | `float` | `float` |  |  | EPO_MIN |
| `epo_mindate` | `timestamp` | `timestamp_ntz` |  |  | EPO_MINDATE |
| `epo_minppm` | `string` | `varchar` |  |  | EPO_MINPPM |
| `epo_minstwo` | `string` | `varchar` |  |  | EPO_MINSTWO |
| `epo_mintol` | `float` | `float` |  |  | EPO_MINTOL |
| `epo_mintoldate` | `timestamp` | `timestamp_ntz` |  |  | EPO_MINTOLDATE |
| `epo_result` | `string` | `varchar` |  |  | EPO_RESULT |
| `epo_rresult` | `string` | `varchar` |  |  | EPO_RRESULT |
| `epo_stwo` | `string` | `varchar` |  |  | EPO_STWO |
| `epo_type` | `string` | `varchar` |  |  | EPO_TYPE |
| `epo_uom` | `string` | `varchar` |  |  | EPO_UOM |
| `epo_value` | `float` | `float` |  |  | EPO_VALUE |
| `epo_object_org` | `string` | `varchar` |  |  | EPO_OBJECT_ORG |
| `epo_inspector` | `string` | `varchar` |  |  | EPO_INSPECTOR |
| `epo_location` | `string` | `varchar` |  |  | EPO_LOCATION |
| `epo_class` | `string` | `varchar` |  |  | EPO_CLASS |
| `epo_class_org` | `string` | `varchar` |  |  | EPO_CLASS_ORG |
| `epo_code` | `string` | `varchar` | `PK` | `unique` | EPO_CODE |
| `epo_origevent` | `string` | `varchar` |  |  | EPO_ORIGEVENT |
| `epo_nidatemincrit` | `timestamp` | `timestamp_ntz` |  |  | EPO_NIDATEMINCRIT |
| `epo_nidatemintol` | `timestamp` | `timestamp_ntz` |  |  | EPO_NIDATEMINTOL |
| `epo_nidatemaxcrit` | `timestamp` | `timestamp_ntz` |  |  | EPO_NIDATEMAXCRIT |
| `epo_nidatemaxtol` | `timestamp` | `timestamp_ntz` |  |  | EPO_NIDATEMAXTOL |
| `epo_assessedslope` | `float` | `float` |  |  | EPO_ASSESSEDSLOPE |
| `epo_varchar1` | `string` | `varchar` |  |  | EPO_VARCHAR1 |
| `epo_varchar2` | `string` | `varchar` |  |  | EPO_VARCHAR2 |
| `epo_varchar3` | `string` | `varchar` |  |  | EPO_VARCHAR3 |
| `epo_varchar4` | `string` | `varchar` |  |  | EPO_VARCHAR4 |
| `epo_varchar5` | `string` | `varchar` |  |  | EPO_VARCHAR5 |
| `epo_varchar6` | `string` | `varchar` |  |  | EPO_VARCHAR6 |
| `epo_varchar7` | `string` | `varchar` |  |  | EPO_VARCHAR7 |
| `epo_varchar8` | `string` | `varchar` |  |  | EPO_VARCHAR8 |
| `epo_varchar9` | `string` | `varchar` |  |  | EPO_VARCHAR9 |
| `epo_varchar10` | `string` | `varchar` |  |  | EPO_VARCHAR10 |
| `epo_varnum1` | `float` | `float` |  |  | EPO_VARNUM1 |
| `epo_varnum2` | `float` | `float` |  |  | EPO_VARNUM2 |
| `epo_varnum3` | `float` | `float` |  |  | EPO_VARNUM3 |
| `epo_varnum4` | `float` | `float` |  |  | EPO_VARNUM4 |
| `epo_varnum5` | `float` | `float` |  |  | EPO_VARNUM5 |
| `epo_varnum6` | `float` | `float` |  |  | EPO_VARNUM6 |
| `epo_varnum7` | `float` | `float` |  |  | EPO_VARNUM7 |
| `epo_varnum8` | `float` | `float` |  |  | EPO_VARNUM8 |
| `epo_varnum9` | `float` | `float` |  |  | EPO_VARNUM9 |
| `epo_varnum10` | `float` | `float` |  |  | EPO_VARNUM10 |
| `epo_vardate1` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE1 |
| `epo_vardate2` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE2 |
| `epo_vardate3` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE3 |
| `epo_vardate4` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE4 |
| `epo_vardate5` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE5 |
| `epo_confrating` | `string` | `varchar` |  |  | EPO_CONFRATING |
| `epo_parent` | `string` | `varchar` |  |  | EPO_PARENT |
| `epo_completed` | `string` | `varchar` |  |  | EPO_COMPLETED |
| `epo_processed` | `string` | `varchar` |  |  | EPO_PROCESSED |
| `epo_risk` | `string` | `varchar` |  |  | EPO_RISK |
| `epo_updatecount` | `float` | `float` |  |  | EPO_UPDATECOUNT |
| `epo_varchar11` | `string` | `varchar` |  |  | EPO_VARCHAR11 |
| `epo_varchar12` | `string` | `varchar` |  |  | EPO_VARCHAR12 |
| `epo_varchar13` | `string` | `varchar` |  |  | EPO_VARCHAR13 |
| `epo_varchar14` | `string` | `varchar` |  |  | EPO_VARCHAR14 |
| `epo_varchar15` | `string` | `varchar` |  |  | EPO_VARCHAR15 |
| `epo_varchar16` | `string` | `varchar` |  |  | EPO_VARCHAR16 |
| `epo_varchar17` | `string` | `varchar` |  |  | EPO_VARCHAR17 |
| `epo_varchar18` | `string` | `varchar` |  |  | EPO_VARCHAR18 |
| `epo_varchar19` | `string` | `varchar` |  |  | EPO_VARCHAR19 |
| `epo_varchar20` | `string` | `varchar` |  |  | EPO_VARCHAR20 |
| `epo_varchar21` | `string` | `varchar` |  |  | EPO_VARCHAR21 |
| `epo_varchar22` | `string` | `varchar` |  |  | EPO_VARCHAR22 |
| `epo_varchar23` | `string` | `varchar` |  |  | EPO_VARCHAR23 |
| `epo_varchar24` | `string` | `varchar` |  |  | EPO_VARCHAR24 |
| `epo_varnum33` | `float` | `float` |  |  | EPO_VARNUM33 |
| `epo_varnum34` | `float` | `float` |  |  | EPO_VARNUM34 |
| `epo_varnum35` | `float` | `float` |  |  | EPO_VARNUM35 |
| `epo_vardate6` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE6 |
| `epo_vardate7` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE7 |
| `epo_vardate8` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE8 |
| `epo_vardate9` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE9 |
| `epo_vardate10` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE10 |
| `epo_vardate11` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE11 |
| `epo_vardate12` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE12 |
| `epo_vardate13` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE13 |
| `epo_vardate14` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE14 |
| `epo_vardate15` | `timestamp` | `timestamp_ntz` |  |  | EPO_VARDATE15 |
| `epo_created` | `timestamp` | `timestamp_ntz` |  |  | EPO_CREATED |
| `epo_updated` | `timestamp` | `timestamp_ntz` |  |  | EPO_UPDATED |
| `epo_frompoint` | `float` | `float` |  |  | EPO_FROMPOINT |
| `epo_fromrefdesc` | `string` | `varchar` |  |  | EPO_FROMREFDESC |
| `epo_fromgeoref` | `string` | `varchar` |  |  | EPO_FROMGEOREF |
| `epo_topoint` | `float` | `float` |  |  | EPO_TOPOINT |
| `epo_torefdesc` | `string` | `varchar` |  |  | EPO_TOREFDESC |
| `epo_togeoref` | `string` | `varchar` |  |  | EPO_TOGEOREF |
| `epo_from_reference` | `float` | `float` |  |  | EPO_FROM_REFERENCE |
| `epo_from_offset` | `float` | `float` |  |  | EPO_FROM_OFFSET |
| `epo_from_offset_percentage` | `float` | `float` |  |  | EPO_FROM_OFFSET_PERCENTAGE |
| `epo_from_offset_direction` | `string` | `varchar` |  |  | EPO_FROM_OFFSET_DIRECTION |
| `epo_from_xcoordinate` | `float` | `float` |  |  | EPO_FROM_XCOORDINATE |
| `epo_from_ycoordinate` | `float` | `float` |  |  | EPO_FROM_YCOORDINATE |
| `epo_from_latitude` | `float` | `float` |  |  | EPO_FROM_LATITUDE |
| `epo_from_longitude` | `float` | `float` |  |  | EPO_FROM_LONGITUDE |
| `epo_from_relationship_type` | `string` | `varchar` |  |  | EPO_FROM_RELATIONSHIP_TYPE |
| `epo_from_horizontal_offset` | `float` | `float` |  |  | EPO_FROM_HORIZONTAL_OFFSET |
| `epo_from_horizontal_offsetuom` | `string` | `varchar` |  |  | EPO_FROM_HORIZONTAL_OFFSETUOM |
| `epo_from_horizontal_offsettype` | `string` | `varchar` |  |  | EPO_FROM_HORIZONTAL_OFFSETTYPE |
| `epo_from_vertical_offset` | `float` | `float` |  |  | EPO_FROM_VERTICAL_OFFSET |
| `epo_from_vertical_offsetuom` | `string` | `varchar` |  |  | EPO_FROM_VERTICAL_OFFSETUOM |
| `epo_from_vertical_offsettype` | `string` | `varchar` |  |  | EPO_FROM_VERTICAL_OFFSETTYPE |
| `epo_to_reference` | `float` | `float` |  |  | EPO_TO_REFERENCE |
| `epo_to_xcoordinate` | `float` | `float` |  |  | EPO_TO_XCOORDINATE |
| `epo_to_offset` | `float` | `float` |  |  | EPO_TO_OFFSET |
| `epo_to_offset_direction` | `string` | `varchar` |  |  | EPO_TO_OFFSET_DIRECTION |
| `epo_to_offset_percentage` | `float` | `float` |  |  | EPO_TO_OFFSET_PERCENTAGE |
| `epo_to_ycoordinate` | `float` | `float` |  |  | EPO_TO_YCOORDINATE |
| `epo_to_latitude` | `float` | `float` |  |  | EPO_TO_LATITUDE |
| `epo_to_longitude` | `float` | `float` |  |  | EPO_TO_LONGITUDE |
| `epo_to_relationship_type` | `string` | `varchar` |  |  | EPO_TO_RELATIONSHIP_TYPE |
| `epo_to_horizontal_offset` | `float` | `float` |  |  | EPO_TO_HORIZONTAL_OFFSET |
| `epo_to_horizontal_offsetuom` | `string` | `varchar` |  |  | EPO_TO_HORIZONTAL_OFFSETUOM |
| `epo_to_horizontal_offsettype` | `string` | `varchar` |  |  | EPO_TO_HORIZONTAL_OFFSETTYPE |
| `epo_to_vertical_offset` | `float` | `float` |  |  | EPO_TO_VERTICAL_OFFSET |
| `epo_to_vertical_offsetuom` | `string` | `varchar` |  |  | EPO_TO_VERTICAL_OFFSETUOM |
| `epo_to_vertical_offsettype` | `string` | `varchar` |  |  | EPO_TO_VERTICAL_OFFSETTYPE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5dwworkorders

eam_R5DWWORKORDERS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dwworkorders` |
| **Materialization** | `incremental` |
| **Column count** | 48 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `zwo_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ZWO_LASTSAVED |
| `zwo_code` | `string` | `varchar` |  |  | ZWO_CODE |
| `zwo_desc` | `string` | `varchar` |  |  | ZWO_DESC |
| `zwo_typecode` | `string` | `varchar` |  |  | ZWO_TYPECODE |
| `zwo_typedesc` | `string` | `varchar` |  |  | ZWO_TYPEDESC |
| `zwo_jobtypecode` | `string` | `varchar` |  |  | ZWO_JOBTYPECODE |
| `zwo_jobtypedesc` | `string` | `varchar` |  |  | ZWO_JOBTYPEDESC |
| `zwo_classcode` | `string` | `varchar` |  |  | ZWO_CLASSCODE |
| `zwo_classorg` | `string` | `varchar` |  |  | ZWO_CLASSORG |
| `zwo_classdesc` | `string` | `varchar` |  |  | ZWO_CLASSDESC |
| `zwo_statuscode` | `string` | `varchar` |  |  | ZWO_STATUSCODE |
| `zwo_statusdesc` | `string` | `varchar` |  |  | ZWO_STATUSDESC |
| `zwo_costcode` | `string` | `varchar` |  |  | ZWO_COSTCODE |
| `zwo_costcodedesc` | `string` | `varchar` |  |  | ZWO_COSTCODEDESC |
| `zwo_locationcode` | `string` | `varchar` |  |  | ZWO_LOCATIONCODE |
| `zwo_locationorg` | `string` | `varchar` |  |  | ZWO_LOCATIONORG |
| `zwo_locationdesc` | `string` | `varchar` |  |  | ZWO_LOCATIONDESC |
| `zwo_projectcode` | `string` | `varchar` |  |  | ZWO_PROJECTCODE |
| `zwo_projectdesc` | `string` | `varchar` |  |  | ZWO_PROJECTDESC |
| `zwo_projbudgetcode` | `string` | `varchar` |  |  | ZWO_PROJBUDGETCODE |
| `zwo_priority` | `string` | `varchar` |  |  | ZWO_PRIORITY |
| `zwo_shiftcode` | `string` | `varchar` |  |  | ZWO_SHIFTCODE |
| `zwo_shiftdesc` | `string` | `varchar` |  |  | ZWO_SHIFTDESC |
| `zwo_assignedbycode` | `string` | `varchar` |  |  | ZWO_ASSIGNEDBYCODE |
| `zwo_assignedbyname` | `string` | `varchar` |  |  | ZWO_ASSIGNEDBYNAME |
| `zwo_assignedtocode` | `string` | `varchar` |  |  | ZWO_ASSIGNEDTOCODE |
| `zwo_assignedtoname` | `string` | `varchar` |  |  | ZWO_ASSIGNEDTONAME |
| `zwo_objunderwarranty` | `string` | `varchar` |  |  | ZWO_OBJUNDERWARRANTY |
| `zwo_reopened` | `string` | `varchar` |  |  | ZWO_REOPENED |
| `zwo_problemcode` | `string` | `varchar` |  |  | ZWO_PROBLEMCODE |
| `zwo_problemdesc` | `string` | `varchar` |  |  | ZWO_PROBLEMDESC |
| `zwo_failurecode` | `string` | `varchar` |  |  | ZWO_FAILURECODE |
| `zwo_failuredesc` | `string` | `varchar` |  |  | ZWO_FAILUREDESC |
| `zwo_causecode` | `string` | `varchar` |  |  | ZWO_CAUSECODE |
| `zwo_causedesc` | `string` | `varchar` |  |  | ZWO_CAUSEDESC |
| `zwo_actioncode` | `string` | `varchar` |  |  | ZWO_ACTIONCODE |
| `zwo_actiondesc` | `string` | `varchar` |  |  | ZWO_ACTIONDESC |
| `zwo_rtypecode` | `string` | `varchar` |  |  | ZWO_RTYPECODE |
| `zwo_rtypedesc` | `string` | `varchar` |  |  | ZWO_RTYPEDESC |
| `zwo_rstatuscode` | `string` | `varchar` |  |  | ZWO_RSTATUSCODE |
| `zwo_rstatusdesc` | `string` | `varchar` |  |  | ZWO_RSTATUSDESC |
| `zwo_key` | `float` | `float` |  |  | ZWO_KEY |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

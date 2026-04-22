# r5dwobjects

eam_R5DWOBJECTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5dwobjects` |
| **Materialization** | `incremental` |
| **Column count** | 53 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `zob_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ZOB_LASTSAVED |
| `zob_variable3` | `string` | `varchar` |  |  | ZOB_VARIABLE3 |
| `zob_variable4` | `string` | `varchar` |  |  | ZOB_VARIABLE4 |
| `zob_variable5` | `string` | `varchar` |  |  | ZOB_VARIABLE5 |
| `zob_variable6` | `string` | `varchar` |  |  | ZOB_VARIABLE6 |
| `zob_key` | `float` | `float` |  |  | ZOB_KEY |
| `zob_code` | `string` | `varchar` |  |  | ZOB_CODE |
| `zob_desc` | `string` | `varchar` |  |  | ZOB_DESC |
| `zob_orgcode` | `string` | `varchar` |  |  | ZOB_ORGCODE |
| `zob_orgdesc` | `string` | `varchar` |  |  | ZOB_ORGDESC |
| `zob_typecode` | `string` | `varchar` |  |  | ZOB_TYPECODE |
| `zob_typedesc` | `string` | `varchar` |  |  | ZOB_TYPEDESC |
| `zob_classcode` | `string` | `varchar` |  |  | ZOB_CLASSCODE |
| `zob_classorg` | `string` | `varchar` |  |  | ZOB_CLASSORG |
| `zob_classdesc` | `string` | `varchar` |  |  | ZOB_CLASSDESC |
| `zob_locationcode` | `string` | `varchar` |  |  | ZOB_LOCATIONCODE |
| `zob_locationorg` | `string` | `varchar` |  |  | ZOB_LOCATIONORG |
| `zob_locationdesc` | `string` | `varchar` |  |  | ZOB_LOCATIONDESC |
| `zob_manufacturercode` | `string` | `varchar` |  |  | ZOB_MANUFACTURERCODE |
| `zob_manufacturerdesc` | `string` | `varchar` |  |  | ZOB_MANUFACTURERDESC |
| `zob_costcode` | `string` | `varchar` |  |  | ZOB_COSTCODE |
| `zob_costcodedesc` | `string` | `varchar` |  |  | ZOB_COSTCODEDESC |
| `zob_statuscode` | `string` | `varchar` |  |  | ZOB_STATUSCODE |
| `zob_statusdesc` | `string` | `varchar` |  |  | ZOB_STATUSDESC |
| `zob_statecode` | `string` | `varchar` |  |  | ZOB_STATECODE |
| `zob_statedesc` | `string` | `varchar` |  |  | ZOB_STATEDESC |
| `zob_productionrelated` | `string` | `varchar` |  |  | ZOB_PRODUCTIONRELATED |
| `zob_safetyrelated` | `string` | `varchar` |  |  | ZOB_SAFETYRELATED |
| `zob_criticality` | `string` | `varchar` |  |  | ZOB_CRITICALITY |
| `zob_valuedefcur` | `float` | `float` |  |  | ZOB_VALUEDEFCUR |
| `zob_defcur` | `string` | `varchar` |  |  | ZOB_DEFCUR |
| `zob_valueorgcur` | `float` | `float` |  |  | ZOB_VALUEORGCUR |
| `zob_orgcur` | `string` | `varchar` |  |  | ZOB_ORGCUR |
| `zob_assignedtocode` | `string` | `varchar` |  |  | ZOB_ASSIGNEDTOCODE |
| `zob_assignedtoname` | `string` | `varchar` |  |  | ZOB_ASSIGNEDTONAME |
| `zob_commissiondate` | `timestamp` | `timestamp_ntz` |  |  | ZOB_COMMISSIONDATE |
| `zob_withdrawaldate` | `timestamp` | `timestamp_ntz` |  |  | ZOB_WITHDRAWALDATE |
| `zob_category` | `string` | `varchar` |  |  | ZOB_CATEGORY |
| `zob_categorydesc` | `string` | `varchar` |  |  | ZOB_CATEGORYDESC |
| `zob_rtypecode` | `string` | `varchar` |  |  | ZOB_RTYPECODE |
| `zob_rtypedesc` | `string` | `varchar` |  |  | ZOB_RTYPEDESC |
| `zob_rstatuscode` | `string` | `varchar` |  |  | ZOB_RSTATUSCODE |
| `zob_rstatusdesc` | `string` | `varchar` |  |  | ZOB_RSTATUSDESC |
| `zob_rstatecode` | `string` | `varchar` |  |  | ZOB_RSTATECODE |
| `zob_rstatedesc` | `string` | `varchar` |  |  | ZOB_RSTATEDESC |
| `zob_variable1` | `string` | `varchar` |  |  | ZOB_VARIABLE1 |
| `zob_variable2` | `string` | `varchar` |  |  | ZOB_VARIABLE2 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

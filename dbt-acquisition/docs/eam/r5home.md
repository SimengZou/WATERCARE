# r5home

eam_R5HOME

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5home` |
| **Materialization** | `incremental` |
| **Primary keys** | `hom_code`, `hom_type` |
| **Column count** | 65 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `hom_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | HOM_LASTSAVED |
| `hom_desc` | `string` | `varchar` |  |  | HOM_DESC |
| `hom_sqlcode` | `string` | `varchar` |  |  | HOM_SQLCODE |
| `hom_function` | `string` | `varchar` |  |  | HOM_FUNCTION |
| `hom_gen` | `string` | `varchar` |  |  | HOM_GEN |
| `hom_parent` | `string` | `varchar` |  |  | HOM_PARENT |
| `hom_updfreq` | `float` | `float` |  |  | HOM_UPDFREQ |
| `hom_update` | `timestamp` | `timestamp_ntz` |  |  | HOM_UPDATE |
| `hom_nxtupdate` | `timestamp` | `timestamp_ntz` |  |  | HOM_NXTUPDATE |
| `hom_uom` | `string` | `varchar` |  |  | HOM_UOM |
| `hom_history` | `string` | `varchar` |  |  | HOM_HISTORY |
| `hom_whereclause` | `string` | `varchar` |  |  | HOM_WHERECLAUSE |
| `hom_curvalue` | `float` | `float` |  |  | HOM_CURVALUE |
| `hom_normscore` | `float` | `float` |  |  | HOM_NORMSCORE |
| `hom_error` | `string` | `varchar` |  |  | HOM_ERROR |
| `hom_updatecount` | `float` | `float` |  |  | HOM_UPDATECOUNT |
| `hom_ewsfunction` | `string` | `varchar` |  |  | HOM_EWSFUNCTION |
| `hom_ewsdataspyid` | `float` | `float` |  |  | HOM_EWSDATASPYID |
| `hom_operator` | `string` | `varchar` |  |  | HOM_OPERATOR |
| `hom_value` | `float` | `float` |  |  | HOM_VALUE |
| `hom_notused` | `string` | `varchar` |  |  | HOM_NOTUSED |
| `hom_rolluptype` | `string` | `varchar` |  |  | HOM_ROLLUPTYPE |
| `hom_udfchar01` | `string` | `varchar` |  |  | HOM_UDFCHAR01 |
| `hom_udfchar02` | `string` | `varchar` |  |  | HOM_UDFCHAR02 |
| `hom_udfchar03` | `string` | `varchar` |  |  | HOM_UDFCHAR03 |
| `hom_udfchar04` | `string` | `varchar` |  |  | HOM_UDFCHAR04 |
| `hom_udfchar05` | `string` | `varchar` |  |  | HOM_UDFCHAR05 |
| `hom_udfchar06` | `string` | `varchar` |  |  | HOM_UDFCHAR06 |
| `hom_udfchar07` | `string` | `varchar` |  |  | HOM_UDFCHAR07 |
| `hom_udfchar08` | `string` | `varchar` |  |  | HOM_UDFCHAR08 |
| `hom_udfchar09` | `string` | `varchar` |  |  | HOM_UDFCHAR09 |
| `hom_udfchar10` | `string` | `varchar` |  |  | HOM_UDFCHAR10 |
| `hom_udfnum01` | `float` | `float` |  |  | HOM_UDFNUM01 |
| `hom_udfnum02` | `float` | `float` |  |  | HOM_UDFNUM02 |
| `hom_udfnum03` | `float` | `float` |  |  | HOM_UDFNUM03 |
| `hom_udfnum04` | `float` | `float` |  |  | HOM_UDFNUM04 |
| `hom_udfnum05` | `float` | `float` |  |  | HOM_UDFNUM05 |
| `hom_udfnum06` | `float` | `float` |  |  | HOM_UDFNUM06 |
| `hom_udfnum07` | `float` | `float` |  |  | HOM_UDFNUM07 |
| `hom_udfnum08` | `float` | `float` |  |  | HOM_UDFNUM08 |
| `hom_udfnum09` | `float` | `float` |  |  | HOM_UDFNUM09 |
| `hom_udfnum10` | `float` | `float` |  |  | HOM_UDFNUM10 |
| `hom_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | HOM_UDFDATE01 |
| `hom_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | HOM_UDFDATE02 |
| `hom_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | HOM_UDFDATE03 |
| `hom_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | HOM_UDFDATE04 |
| `hom_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | HOM_UDFDATE05 |
| `hom_udfchkbox01` | `string` | `varchar` |  |  | HOM_UDFCHKBOX01 |
| `hom_udfchkbox02` | `string` | `varchar` |  |  | HOM_UDFCHKBOX02 |
| `hom_udfchkbox03` | `string` | `varchar` |  |  | HOM_UDFCHKBOX03 |
| `hom_udfchkbox04` | `string` | `varchar` |  |  | HOM_UDFCHKBOX04 |
| `hom_udfchkbox05` | `string` | `varchar` |  |  | HOM_UDFCHKBOX05 |
| `hom_min` | `float` | `float` |  |  | HOM_MIN |
| `hom_max` | `float` | `float` |  |  | HOM_MAX |
| `hom_radiuspercentage` | `float` | `float` |  |  | HOM_RADIUSPERCENTAGE |
| `hom_kpitype` | `string` | `varchar` |  |  | HOM_KPITYPE |
| `hom_groupbytext` | `string` | `varchar` |  |  | HOM_GROUPBYTEXT |
| `hom_code` | `string` | `varchar` | `PK` |  | HOM_CODE |
| `hom_type` | `string` | `varchar` | `PK` |  | HOM_TYPE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

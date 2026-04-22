# r5organization

eam_R5ORGANIZATION

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5organization` |
| **Materialization** | `incremental` |
| **Primary keys** | `org_code` |
| **Column count** | 58 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `org_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ORG_LASTSAVED |
| `org_requestrecalcdep` | `string` | `varchar` |  |  | ORG_REQUESTRECALCDEP |
| `org_curr` | `string` | `varchar` |  |  | ORG_CURR |
| `org_common` | `string` | `varchar` |  |  | ORG_COMMON |
| `org_coderef` | `string` | `varchar` |  |  | ORG_CODEREF |
| `org_bookid` | `string` | `varchar` |  |  | ORG_BOOKID |
| `org_matchlta` | `float` | `float` |  |  | ORG_MATCHLTA |
| `org_matchltp` | `float` | `float` |  |  | ORG_MATCHLTP |
| `org_timezone` | `float` | `float` |  |  | ORG_TIMEZONE |
| `org_latitude` | `float` | `float` |  |  | ORG_LATITUDE |
| `org_longitude` | `float` | `float` |  |  | ORG_LONGITUDE |
| `org_externalorg` | `string` | `varchar` |  |  | ORG_EXTERNALORG |
| `org_invqtytol` | `float` | `float` |  |  | ORG_INVQTYTOL |
| `org_dunsnum` | `string` | `varchar` |  |  | ORG_DUNSNUM |
| `org_updatecount` | `float` | `float` |  |  | ORG_UPDATECOUNT |
| `org_created` | `timestamp` | `timestamp_ntz` |  |  | ORG_CREATED |
| `org_updated` | `timestamp` | `timestamp_ntz` |  |  | ORG_UPDATED |
| `org_locale` | `string` | `varchar` |  |  | ORG_LOCALE |
| `org_depmethod` | `string` | `varchar` |  |  | ORG_DEPMETHOD |
| `org_deprmethod` | `string` | `varchar` |  |  | ORG_DEPRMETHOD |
| `org_segmentvalue` | `string` | `varchar` |  |  | ORG_SEGMENTVALUE |
| `org_udfchar01` | `string` | `varchar` |  |  | ORG_UDFCHAR01 |
| `org_udfchar02` | `string` | `varchar` |  |  | ORG_UDFCHAR02 |
| `org_udfchar03` | `string` | `varchar` |  |  | ORG_UDFCHAR03 |
| `org_udfchar04` | `string` | `varchar` |  |  | ORG_UDFCHAR04 |
| `org_udfchar05` | `string` | `varchar` |  |  | ORG_UDFCHAR05 |
| `org_udfchar06` | `string` | `varchar` |  |  | ORG_UDFCHAR06 |
| `org_udfchar07` | `string` | `varchar` |  |  | ORG_UDFCHAR07 |
| `org_udfchar08` | `string` | `varchar` |  |  | ORG_UDFCHAR08 |
| `org_udfchar09` | `string` | `varchar` |  |  | ORG_UDFCHAR09 |
| `org_udfchar10` | `string` | `varchar` |  |  | ORG_UDFCHAR10 |
| `org_udfnum01` | `float` | `float` |  |  | ORG_UDFNUM01 |
| `org_udfnum02` | `float` | `float` |  |  | ORG_UDFNUM02 |
| `org_udfnum03` | `float` | `float` |  |  | ORG_UDFNUM03 |
| `org_udfnum04` | `float` | `float` |  |  | ORG_UDFNUM04 |
| `org_udfnum05` | `float` | `float` |  |  | ORG_UDFNUM05 |
| `org_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | ORG_UDFDATE01 |
| `org_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | ORG_UDFDATE02 |
| `org_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | ORG_UDFDATE03 |
| `org_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | ORG_UDFDATE04 |
| `org_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | ORG_UDFDATE05 |
| `org_udfchkbox01` | `string` | `varchar` |  |  | ORG_UDFCHKBOX01 |
| `org_udfchkbox02` | `string` | `varchar` |  |  | ORG_UDFCHKBOX02 |
| `org_udfchkbox03` | `string` | `varchar` |  |  | ORG_UDFCHKBOX03 |
| `org_udfchkbox04` | `string` | `varchar` |  |  | ORG_UDFCHKBOX04 |
| `org_udfchkbox05` | `string` | `varchar` |  |  | ORG_UDFCHKBOX05 |
| `org_accountingentity` | `string` | `varchar` |  |  | ORG_ACCOUNTINGENTITY |
| `org_streamplusproject` | `string` | `varchar` |  |  | ORG_STREAMPLUSPROJECT |
| `org_calgroupcode` | `string` | `varchar` |  |  | ORG_CALGROUPCODE |
| `org_calgrouporg` | `string` | `varchar` |  |  | ORG_CALGROUPORG |
| `org_code` | `string` | `varchar` | `PK` | `unique` | ORG_CODE |
| `org_desc` | `string` | `varchar` |  |  | ORG_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

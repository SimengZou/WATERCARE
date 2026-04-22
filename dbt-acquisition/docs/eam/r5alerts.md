# r5alerts

eam_R5ALERTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5alerts` |
| **Materialization** | `incremental` |
| **Primary keys** | `alt_code` |
| **Column count** | 67 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `alt_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ALT_LASTSAVED |
| `alt_udfchar01` | `string` | `varchar` |  |  | ALT_UDFCHAR01 |
| `alt_gridid` | `float` | `float` |  |  | ALT_GRIDID |
| `alt_dataspyid` | `float` | `float` |  |  | ALT_DATASPYID |
| `alt_freq` | `float` | `float` |  |  | ALT_FREQ |
| `alt_frequom` | `string` | `varchar` |  |  | ALT_FREQUOM |
| `alt_nexteval` | `timestamp` | `timestamp_ntz` |  |  | ALT_NEXTEVAL |
| `alt_lasteval` | `timestamp` | `timestamp_ntz` |  |  | ALT_LASTEVAL |
| `alt_lastalert` | `timestamp` | `timestamp_ntz` |  |  | ALT_LASTALERT |
| `alt_useminmax` | `string` | `varchar` |  |  | ALT_USEMINMAX |
| `alt_minvalue` | `float` | `float` |  |  | ALT_MINVALUE |
| `alt_maxvalue` | `float` | `float` |  |  | ALT_MAXVALUE |
| `alt_withinvalues` | `string` | `varchar` |  |  | ALT_WITHINVALUES |
| `alt_valuefieldid` | `float` | `float` |  |  | ALT_VALUEFIELDID |
| `alt_rentity` | `string` | `varchar` |  |  | ALT_RENTITY |
| `alt_entity` | `string` | `varchar` |  |  | ALT_ENTITY |
| `alt_codefieldid` | `float` | `float` |  |  | ALT_CODEFIELDID |
| `alt_orgfieldid` | `float` | `float` |  |  | ALT_ORGFIELDID |
| `alt_active` | `string` | `varchar` |  |  | ALT_ACTIVE |
| `alt_enablewo` | `string` | `varchar` |  |  | ALT_ENABLEWO |
| `alt_enablemail` | `string` | `varchar` |  |  | ALT_ENABLEMAIL |
| `alt_inprogress` | `string` | `varchar` |  |  | ALT_INPROGRESS |
| `alt_udfchar02` | `string` | `varchar` |  |  | ALT_UDFCHAR02 |
| `alt_udfchar03` | `string` | `varchar` |  |  | ALT_UDFCHAR03 |
| `alt_udfchar04` | `string` | `varchar` |  |  | ALT_UDFCHAR04 |
| `alt_udfchar05` | `string` | `varchar` |  |  | ALT_UDFCHAR05 |
| `alt_udfchar06` | `string` | `varchar` |  |  | ALT_UDFCHAR06 |
| `alt_udfchar07` | `string` | `varchar` |  |  | ALT_UDFCHAR07 |
| `alt_udfchar08` | `string` | `varchar` |  |  | ALT_UDFCHAR08 |
| `alt_udfchar09` | `string` | `varchar` |  |  | ALT_UDFCHAR09 |
| `alt_udfchar10` | `string` | `varchar` |  |  | ALT_UDFCHAR10 |
| `alt_udfnum01` | `float` | `float` |  |  | ALT_UDFNUM01 |
| `alt_udfnum02` | `float` | `float` |  |  | ALT_UDFNUM02 |
| `alt_udfnum03` | `float` | `float` |  |  | ALT_UDFNUM03 |
| `alt_udfnum04` | `float` | `float` |  |  | ALT_UDFNUM04 |
| `alt_udfnum05` | `float` | `float` |  |  | ALT_UDFNUM05 |
| `alt_udfnum06` | `float` | `float` |  |  | ALT_UDFNUM06 |
| `alt_udfnum07` | `float` | `float` |  |  | ALT_UDFNUM07 |
| `alt_udfnum08` | `float` | `float` |  |  | ALT_UDFNUM08 |
| `alt_udfnum09` | `float` | `float` |  |  | ALT_UDFNUM09 |
| `alt_udfnum10` | `float` | `float` |  |  | ALT_UDFNUM10 |
| `alt_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | ALT_UDFDATE01 |
| `alt_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | ALT_UDFDATE02 |
| `alt_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | ALT_UDFDATE03 |
| `alt_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | ALT_UDFDATE04 |
| `alt_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | ALT_UDFDATE05 |
| `alt_udfchkbox01` | `string` | `varchar` |  |  | ALT_UDFCHKBOX01 |
| `alt_udfchkbox02` | `string` | `varchar` |  |  | ALT_UDFCHKBOX02 |
| `alt_udfchkbox03` | `string` | `varchar` |  |  | ALT_UDFCHKBOX03 |
| `alt_udfchkbox04` | `string` | `varchar` |  |  | ALT_UDFCHKBOX04 |
| `alt_udfchkbox05` | `string` | `varchar` |  |  | ALT_UDFCHKBOX05 |
| `alt_created` | `timestamp` | `timestamp_ntz` |  |  | ALT_CREATED |
| `alt_createdby` | `string` | `varchar` |  |  | ALT_CREATEDBY |
| `alt_updatecount` | `float` | `float` |  |  | ALT_UPDATECOUNT |
| `alt_source` | `string` | `varchar` |  |  | ALT_SOURCE |
| `alt_enableionpulse` | `string` | `varchar` |  |  | ALT_ENABLEIONPULSE |
| `alt_enablegeneratewo` | `string` | `varchar` |  |  | ALT_ENABLEGENERATEWO |
| `alt_import_ref` | `string` | `varchar` |  |  | ALT_IMPORT_REF |
| `alt_enablewopickticket` | `string` | `varchar` |  |  | ALT_ENABLEWOPICKTICKET |
| `alt_code` | `string` | `varchar` | `PK` | `unique` | ALT_CODE |
| `alt_desc` | `string` | `varchar` |  |  | ALT_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

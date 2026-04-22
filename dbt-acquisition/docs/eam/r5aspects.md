# r5aspects

eam_R5ASPECTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5aspects` |
| **Materialization** | `incremental` |
| **Primary keys** | `asp_code` |
| **Column count** | 63 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `asp_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ASP_LASTSAVED |
| `asp_class` | `string` | `varchar` |  |  | ASP_CLASS |
| `asp_timedep` | `string` | `varchar` |  |  | ASP_TIMEDEP |
| `asp_class_org` | `string` | `varchar` |  |  | ASP_CLASS_ORG |
| `asp_parent` | `string` | `varchar` |  |  | ASP_PARENT |
| `asp_notused` | `string` | `varchar` |  |  | ASP_NOTUSED |
| `asp_random` | `string` | `varchar` |  |  | ASP_RANDOM |
| `asp_updatecount` | `float` | `float` |  |  | ASP_UPDATECOUNT |
| `asp_created` | `timestamp` | `timestamp_ntz` |  |  | ASP_CREATED |
| `asp_updated` | `timestamp` | `timestamp_ntz` |  |  | ASP_UPDATED |
| `asp_udfchar01` | `string` | `varchar` |  |  | ASP_UDFCHAR01 |
| `asp_udfchar02` | `string` | `varchar` |  |  | ASP_UDFCHAR02 |
| `asp_udfchar03` | `string` | `varchar` |  |  | ASP_UDFCHAR03 |
| `asp_udfchar04` | `string` | `varchar` |  |  | ASP_UDFCHAR04 |
| `asp_udfchar05` | `string` | `varchar` |  |  | ASP_UDFCHAR05 |
| `asp_udfchar06` | `string` | `varchar` |  |  | ASP_UDFCHAR06 |
| `asp_udfchar07` | `string` | `varchar` |  |  | ASP_UDFCHAR07 |
| `asp_udfchar08` | `string` | `varchar` |  |  | ASP_UDFCHAR08 |
| `asp_udfchar09` | `string` | `varchar` |  |  | ASP_UDFCHAR09 |
| `asp_udfchar10` | `string` | `varchar` |  |  | ASP_UDFCHAR10 |
| `asp_udfchar11` | `string` | `varchar` |  |  | ASP_UDFCHAR11 |
| `asp_udfchar12` | `string` | `varchar` |  |  | ASP_UDFCHAR12 |
| `asp_udfchar13` | `string` | `varchar` |  |  | ASP_UDFCHAR13 |
| `asp_udfchar14` | `string` | `varchar` |  |  | ASP_UDFCHAR14 |
| `asp_udfchar15` | `string` | `varchar` |  |  | ASP_UDFCHAR15 |
| `asp_udfchar16` | `string` | `varchar` |  |  | ASP_UDFCHAR16 |
| `asp_udfchar17` | `string` | `varchar` |  |  | ASP_UDFCHAR17 |
| `asp_udfchar18` | `string` | `varchar` |  |  | ASP_UDFCHAR18 |
| `asp_udfchar19` | `string` | `varchar` |  |  | ASP_UDFCHAR19 |
| `asp_udfchar20` | `string` | `varchar` |  |  | ASP_UDFCHAR20 |
| `asp_udfchar21` | `string` | `varchar` |  |  | ASP_UDFCHAR21 |
| `asp_udfchar22` | `string` | `varchar` |  |  | ASP_UDFCHAR22 |
| `asp_udfchar23` | `string` | `varchar` |  |  | ASP_UDFCHAR23 |
| `asp_udfchar24` | `string` | `varchar` |  |  | ASP_UDFCHAR24 |
| `asp_udfchar25` | `string` | `varchar` |  |  | ASP_UDFCHAR25 |
| `asp_udfchar26` | `string` | `varchar` |  |  | ASP_UDFCHAR26 |
| `asp_udfchar27` | `string` | `varchar` |  |  | ASP_UDFCHAR27 |
| `asp_udfchar28` | `string` | `varchar` |  |  | ASP_UDFCHAR28 |
| `asp_udfchar29` | `string` | `varchar` |  |  | ASP_UDFCHAR29 |
| `asp_udfchar30` | `string` | `varchar` |  |  | ASP_UDFCHAR30 |
| `asp_udfnum01` | `float` | `float` |  |  | ASP_UDFNUM01 |
| `asp_udfnum02` | `float` | `float` |  |  | ASP_UDFNUM02 |
| `asp_udfnum03` | `float` | `float` |  |  | ASP_UDFNUM03 |
| `asp_udfnum04` | `float` | `float` |  |  | ASP_UDFNUM04 |
| `asp_udfnum05` | `float` | `float` |  |  | ASP_UDFNUM05 |
| `asp_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | ASP_UDFDATE01 |
| `asp_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | ASP_UDFDATE02 |
| `asp_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | ASP_UDFDATE03 |
| `asp_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | ASP_UDFDATE04 |
| `asp_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | ASP_UDFDATE05 |
| `asp_udfchkbox01` | `string` | `varchar` |  |  | ASP_UDFCHKBOX01 |
| `asp_udfchkbox02` | `string` | `varchar` |  |  | ASP_UDFCHKBOX02 |
| `asp_udfchkbox03` | `string` | `varchar` |  |  | ASP_UDFCHKBOX03 |
| `asp_udfchkbox04` | `string` | `varchar` |  |  | ASP_UDFCHKBOX04 |
| `asp_udfchkbox05` | `string` | `varchar` |  |  | ASP_UDFCHKBOX05 |
| `asp_code` | `string` | `varchar` | `PK` | `unique` | ASP_CODE |
| `asp_desc` | `string` | `varchar` |  |  | ASP_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

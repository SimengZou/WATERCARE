# r5adjustments

eam_R5ADJUSTMENTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5adjustments` |
| **Materialization** | `incremental` |
| **Primary keys** | `adj_code`, `adj_org` |
| **Column count** | 59 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `adj_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ADJ_LASTSAVED |
| `adj_code` | `string` | `varchar` | `PK` |  | ADJ_CODE |
| `adj_rate` | `float` | `float` |  |  | ADJ_RATE |
| `adj_stwo` | `string` | `varchar` |  |  | ADJ_STWO |
| `adj_notused` | `string` | `varchar` |  |  | ADJ_NOTUSED |
| `adj_udfchar01` | `string` | `varchar` |  |  | ADJ_UDFCHAR01 |
| `adj_udfchar02` | `string` | `varchar` |  |  | ADJ_UDFCHAR02 |
| `adj_udfchar03` | `string` | `varchar` |  |  | ADJ_UDFCHAR03 |
| `adj_udfchar04` | `string` | `varchar` |  |  | ADJ_UDFCHAR04 |
| `adj_udfchar05` | `string` | `varchar` |  |  | ADJ_UDFCHAR05 |
| `adj_udfchar06` | `string` | `varchar` |  |  | ADJ_UDFCHAR06 |
| `adj_udfchar07` | `string` | `varchar` |  |  | ADJ_UDFCHAR07 |
| `adj_udfchar08` | `string` | `varchar` |  |  | ADJ_UDFCHAR08 |
| `adj_udfchar09` | `string` | `varchar` |  |  | ADJ_UDFCHAR09 |
| `adj_udfchar10` | `string` | `varchar` |  |  | ADJ_UDFCHAR10 |
| `adj_udfchar11` | `string` | `varchar` |  |  | ADJ_UDFCHAR11 |
| `adj_udfchar12` | `string` | `varchar` |  |  | ADJ_UDFCHAR12 |
| `adj_udfchar13` | `string` | `varchar` |  |  | ADJ_UDFCHAR13 |
| `adj_udfchar14` | `string` | `varchar` |  |  | ADJ_UDFCHAR14 |
| `adj_udfchar15` | `string` | `varchar` |  |  | ADJ_UDFCHAR15 |
| `adj_udfchar16` | `string` | `varchar` |  |  | ADJ_UDFCHAR16 |
| `adj_udfchar17` | `string` | `varchar` |  |  | ADJ_UDFCHAR17 |
| `adj_udfchar18` | `string` | `varchar` |  |  | ADJ_UDFCHAR18 |
| `adj_udfchar19` | `string` | `varchar` |  |  | ADJ_UDFCHAR19 |
| `adj_udfchar20` | `string` | `varchar` |  |  | ADJ_UDFCHAR20 |
| `adj_udfchar21` | `string` | `varchar` |  |  | ADJ_UDFCHAR21 |
| `adj_udfchar22` | `string` | `varchar` |  |  | ADJ_UDFCHAR22 |
| `adj_udfchar23` | `string` | `varchar` |  |  | ADJ_UDFCHAR23 |
| `adj_udfchar24` | `string` | `varchar` |  |  | ADJ_UDFCHAR24 |
| `adj_udfchar25` | `string` | `varchar` |  |  | ADJ_UDFCHAR25 |
| `adj_udfchar26` | `string` | `varchar` |  |  | ADJ_UDFCHAR26 |
| `adj_udfchar27` | `string` | `varchar` |  |  | ADJ_UDFCHAR27 |
| `adj_udfchar28` | `string` | `varchar` |  |  | ADJ_UDFCHAR28 |
| `adj_udfchar29` | `string` | `varchar` |  |  | ADJ_UDFCHAR29 |
| `adj_udfchar30` | `string` | `varchar` |  |  | ADJ_UDFCHAR30 |
| `adj_udfnum01` | `float` | `float` |  |  | ADJ_UDFNUM01 |
| `adj_udfnum02` | `float` | `float` |  |  | ADJ_UDFNUM02 |
| `adj_udfnum03` | `float` | `float` |  |  | ADJ_UDFNUM03 |
| `adj_udfnum04` | `float` | `float` |  |  | ADJ_UDFNUM04 |
| `adj_udfnum05` | `float` | `float` |  |  | ADJ_UDFNUM05 |
| `adj_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | ADJ_UDFDATE01 |
| `adj_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | ADJ_UDFDATE02 |
| `adj_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | ADJ_UDFDATE03 |
| `adj_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | ADJ_UDFDATE04 |
| `adj_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | ADJ_UDFDATE05 |
| `adj_udfchkbox01` | `string` | `varchar` |  |  | ADJ_UDFCHKBOX01 |
| `adj_udfchkbox02` | `string` | `varchar` |  |  | ADJ_UDFCHKBOX02 |
| `adj_udfchkbox03` | `string` | `varchar` |  |  | ADJ_UDFCHKBOX03 |
| `adj_udfchkbox04` | `string` | `varchar` |  |  | ADJ_UDFCHKBOX04 |
| `adj_udfchkbox05` | `string` | `varchar` |  |  | ADJ_UDFCHKBOX05 |
| `adj_updatecount` | `float` | `float` |  |  | ADJ_UPDATECOUNT |
| `adj_org` | `string` | `varchar` | `PK` |  | ADJ_ORG |
| `adj_desc` | `string` | `varchar` |  |  | ADJ_DESC |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

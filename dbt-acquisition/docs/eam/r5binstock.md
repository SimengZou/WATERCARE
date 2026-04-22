# r5binstock

eam_R5BINSTOCK

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5binstock` |
| **Materialization** | `incremental` |
| **Primary keys** | `bis_store`, `bis_part`, `bis_part_org`, `bis_bin`, `bis_lot` |
| **Column count** | 64 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `bis_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | BIS_LASTSAVED |
| `bis_bin` | `string` | `varchar` | `PK` |  | BIS_BIN |
| `bis_lot` | `string` | `varchar` | `PK` |  | BIS_LOT |
| `bis_qty` | `float` | `float` |  |  | BIS_QTY |
| `bis_part_org` | `string` | `varchar` | `PK` |  | BIS_PART_ORG |
| `bis_repairqty` | `float` | `float` |  |  | BIS_REPAIRQTY |
| `bis_created` | `timestamp` | `timestamp_ntz` |  |  | BIS_CREATED |
| `bis_createdby` | `string` | `varchar` |  |  | BIS_CREATEDBY |
| `bis_updated` | `timestamp` | `timestamp_ntz` |  |  | BIS_UPDATED |
| `bis_updatedby` | `string` | `varchar` |  |  | BIS_UPDATEDBY |
| `bis_updatecount` | `float` | `float` |  |  | BIS_UPDATECOUNT |
| `bis_udfchar01` | `string` | `varchar` |  |  | BIS_UDFCHAR01 |
| `bis_udfchar02` | `string` | `varchar` |  |  | BIS_UDFCHAR02 |
| `bis_udfchar03` | `string` | `varchar` |  |  | BIS_UDFCHAR03 |
| `bis_udfchar04` | `string` | `varchar` |  |  | BIS_UDFCHAR04 |
| `bis_udfchar05` | `string` | `varchar` |  |  | BIS_UDFCHAR05 |
| `bis_udfchar06` | `string` | `varchar` |  |  | BIS_UDFCHAR06 |
| `bis_udfchar07` | `string` | `varchar` |  |  | BIS_UDFCHAR07 |
| `bis_udfchar08` | `string` | `varchar` |  |  | BIS_UDFCHAR08 |
| `bis_udfchar09` | `string` | `varchar` |  |  | BIS_UDFCHAR09 |
| `bis_udfchar10` | `string` | `varchar` |  |  | BIS_UDFCHAR10 |
| `bis_udfchar11` | `string` | `varchar` |  |  | BIS_UDFCHAR11 |
| `bis_udfchar12` | `string` | `varchar` |  |  | BIS_UDFCHAR12 |
| `bis_udfchar13` | `string` | `varchar` |  |  | BIS_UDFCHAR13 |
| `bis_udfchar14` | `string` | `varchar` |  |  | BIS_UDFCHAR14 |
| `bis_udfchar15` | `string` | `varchar` |  |  | BIS_UDFCHAR15 |
| `bis_udfchar16` | `string` | `varchar` |  |  | BIS_UDFCHAR16 |
| `bis_udfchar17` | `string` | `varchar` |  |  | BIS_UDFCHAR17 |
| `bis_udfchar18` | `string` | `varchar` |  |  | BIS_UDFCHAR18 |
| `bis_udfchar19` | `string` | `varchar` |  |  | BIS_UDFCHAR19 |
| `bis_udfchar20` | `string` | `varchar` |  |  | BIS_UDFCHAR20 |
| `bis_udfchar21` | `string` | `varchar` |  |  | BIS_UDFCHAR21 |
| `bis_udfchar22` | `string` | `varchar` |  |  | BIS_UDFCHAR22 |
| `bis_udfchar23` | `string` | `varchar` |  |  | BIS_UDFCHAR23 |
| `bis_udfchar24` | `string` | `varchar` |  |  | BIS_UDFCHAR24 |
| `bis_udfchar25` | `string` | `varchar` |  |  | BIS_UDFCHAR25 |
| `bis_udfchar26` | `string` | `varchar` |  |  | BIS_UDFCHAR26 |
| `bis_udfchar27` | `string` | `varchar` |  |  | BIS_UDFCHAR27 |
| `bis_udfchar28` | `string` | `varchar` |  |  | BIS_UDFCHAR28 |
| `bis_udfchar29` | `string` | `varchar` |  |  | BIS_UDFCHAR29 |
| `bis_udfchar30` | `string` | `varchar` |  |  | BIS_UDFCHAR30 |
| `bis_udfnum01` | `float` | `float` |  |  | BIS_UDFNUM01 |
| `bis_udfnum02` | `float` | `float` |  |  | BIS_UDFNUM02 |
| `bis_udfnum03` | `float` | `float` |  |  | BIS_UDFNUM03 |
| `bis_udfnum04` | `float` | `float` |  |  | BIS_UDFNUM04 |
| `bis_udfnum05` | `float` | `float` |  |  | BIS_UDFNUM05 |
| `bis_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | BIS_UDFDATE01 |
| `bis_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | BIS_UDFDATE02 |
| `bis_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | BIS_UDFDATE03 |
| `bis_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | BIS_UDFDATE04 |
| `bis_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | BIS_UDFDATE05 |
| `bis_udfchkbox01` | `string` | `varchar` |  |  | BIS_UDFCHKBOX01 |
| `bis_udfchkbox02` | `string` | `varchar` |  |  | BIS_UDFCHKBOX02 |
| `bis_udfchkbox03` | `string` | `varchar` |  |  | BIS_UDFCHKBOX03 |
| `bis_udfchkbox04` | `string` | `varchar` |  |  | BIS_UDFCHKBOX04 |
| `bis_udfchkbox05` | `string` | `varchar` |  |  | BIS_UDFCHKBOX05 |
| `bis_part` | `string` | `varchar` | `PK` |  | BIS_PART |
| `bis_store` | `string` | `varchar` | `PK` |  | BIS_STORE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

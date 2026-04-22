# r5points

eam_R5POINTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5points` |
| **Materialization** | `incremental` |
| **Primary keys** | `poi_object`, `poi_object_org`, `poi_obtype`, `poi_point`, `poi_pointtype` |
| **Column count** | 62 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `poi_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | POI_LASTSAVED |
| `poi_obtype` | `string` | `varchar` | `PK` |  | POI_OBTYPE |
| `poi_point` | `string` | `varchar` | `PK` |  | POI_POINT |
| `poi_desc` | `string` | `varchar` |  |  | POI_DESC |
| `poi_pointtype` | `string` | `varchar` | `PK` |  | POI_POINTTYPE |
| `poi_object_org` | `string` | `varchar` | `PK` |  | POI_OBJECT_ORG |
| `poi_updatecount` | `float` | `float` |  |  | POI_UPDATECOUNT |
| `poi_created` | `timestamp` | `timestamp_ntz` |  |  | POI_CREATED |
| `poi_updated` | `timestamp` | `timestamp_ntz` |  |  | POI_UPDATED |
| `poi_udfchar01` | `string` | `varchar` |  |  | POI_UDFCHAR01 |
| `poi_udfchar02` | `string` | `varchar` |  |  | POI_UDFCHAR02 |
| `poi_udfchar03` | `string` | `varchar` |  |  | POI_UDFCHAR03 |
| `poi_udfchar04` | `string` | `varchar` |  |  | POI_UDFCHAR04 |
| `poi_udfchar05` | `string` | `varchar` |  |  | POI_UDFCHAR05 |
| `poi_udfchar06` | `string` | `varchar` |  |  | POI_UDFCHAR06 |
| `poi_udfchar07` | `string` | `varchar` |  |  | POI_UDFCHAR07 |
| `poi_udfchar08` | `string` | `varchar` |  |  | POI_UDFCHAR08 |
| `poi_udfchar09` | `string` | `varchar` |  |  | POI_UDFCHAR09 |
| `poi_udfchar10` | `string` | `varchar` |  |  | POI_UDFCHAR10 |
| `poi_udfchar11` | `string` | `varchar` |  |  | POI_UDFCHAR11 |
| `poi_udfchar12` | `string` | `varchar` |  |  | POI_UDFCHAR12 |
| `poi_udfchar13` | `string` | `varchar` |  |  | POI_UDFCHAR13 |
| `poi_udfchar14` | `string` | `varchar` |  |  | POI_UDFCHAR14 |
| `poi_udfchar15` | `string` | `varchar` |  |  | POI_UDFCHAR15 |
| `poi_udfchar16` | `string` | `varchar` |  |  | POI_UDFCHAR16 |
| `poi_udfchar17` | `string` | `varchar` |  |  | POI_UDFCHAR17 |
| `poi_udfchar18` | `string` | `varchar` |  |  | POI_UDFCHAR18 |
| `poi_udfchar19` | `string` | `varchar` |  |  | POI_UDFCHAR19 |
| `poi_udfchar20` | `string` | `varchar` |  |  | POI_UDFCHAR20 |
| `poi_udfchar21` | `string` | `varchar` |  |  | POI_UDFCHAR21 |
| `poi_udfchar22` | `string` | `varchar` |  |  | POI_UDFCHAR22 |
| `poi_udfchar23` | `string` | `varchar` |  |  | POI_UDFCHAR23 |
| `poi_udfchar24` | `string` | `varchar` |  |  | POI_UDFCHAR24 |
| `poi_udfchar25` | `string` | `varchar` |  |  | POI_UDFCHAR25 |
| `poi_udfchar26` | `string` | `varchar` |  |  | POI_UDFCHAR26 |
| `poi_udfchar27` | `string` | `varchar` |  |  | POI_UDFCHAR27 |
| `poi_udfchar28` | `string` | `varchar` |  |  | POI_UDFCHAR28 |
| `poi_udfchar29` | `string` | `varchar` |  |  | POI_UDFCHAR29 |
| `poi_udfchar30` | `string` | `varchar` |  |  | POI_UDFCHAR30 |
| `poi_udfnum01` | `float` | `float` |  |  | POI_UDFNUM01 |
| `poi_udfnum02` | `float` | `float` |  |  | POI_UDFNUM02 |
| `poi_udfnum03` | `float` | `float` |  |  | POI_UDFNUM03 |
| `poi_udfnum04` | `float` | `float` |  |  | POI_UDFNUM04 |
| `poi_udfnum05` | `float` | `float` |  |  | POI_UDFNUM05 |
| `poi_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | POI_UDFDATE01 |
| `poi_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | POI_UDFDATE02 |
| `poi_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | POI_UDFDATE03 |
| `poi_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | POI_UDFDATE04 |
| `poi_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | POI_UDFDATE05 |
| `poi_udfchkbox01` | `string` | `varchar` |  |  | POI_UDFCHKBOX01 |
| `poi_udfchkbox02` | `string` | `varchar` |  |  | POI_UDFCHKBOX02 |
| `poi_udfchkbox03` | `string` | `varchar` |  |  | POI_UDFCHKBOX03 |
| `poi_udfchkbox04` | `string` | `varchar` |  |  | POI_UDFCHKBOX04 |
| `poi_udfchkbox05` | `string` | `varchar` |  |  | POI_UDFCHKBOX05 |
| `poi_object` | `string` | `varchar` | `PK` |  | POI_OBJECT |
| `poi_obrtype` | `string` | `varchar` |  |  | POI_OBRTYPE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

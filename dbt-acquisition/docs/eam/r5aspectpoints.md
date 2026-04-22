# r5aspectpoints

eam_R5ASPECTPOINTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5aspectpoints` |
| **Materialization** | `incremental` |
| **Primary keys** | `apo_object`, `apo_object_org`, `apo_obtype`, `apo_aspect`, `apo_point`, `apo_pointtype` |
| **Column count** | 80 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `apo_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | APO_LASTSAVED |
| `apo_object` | `string` | `varchar` | `PK` |  | APO_OBJECT |
| `apo_obrtype` | `string` | `varchar` |  |  | APO_OBRTYPE |
| `apo_obtype` | `string` | `varchar` | `PK` |  | APO_OBTYPE |
| `apo_aspect` | `string` | `varchar` | `PK` |  | APO_ASPECT |
| `apo_point` | `string` | `varchar` | `PK` |  | APO_POINT |
| `apo_pointtype` | `string` | `varchar` | `PK` |  | APO_POINTTYPE |
| `apo_max` | `float` | `float` |  |  | APO_MAX |
| `apo_maxextr` | `float` | `float` |  |  | APO_MAXEXTR |
| `apo_maxppm` | `string` | `varchar` |  |  | APO_MAXPPM |
| `apo_maxstwo` | `string` | `varchar` |  |  | APO_MAXSTWO |
| `apo_maxtol` | `float` | `float` |  |  | APO_MAXTOL |
| `apo_method` | `string` | `varchar` |  |  | APO_METHOD |
| `apo_min` | `float` | `float` |  |  | APO_MIN |
| `apo_minextr` | `float` | `float` |  |  | APO_MINEXTR |
| `apo_minppm` | `string` | `varchar` |  |  | APO_MINPPM |
| `apo_minstwo` | `string` | `varchar` |  |  | APO_MINSTWO |
| `apo_mintol` | `float` | `float` |  |  | APO_MINTOL |
| `apo_nominal` | `float` | `float` |  |  | APO_NOMINAL |
| `apo_uom` | `string` | `varchar` |  |  | APO_UOM |
| `apo_object_org` | `string` | `varchar` | `PK` |  | APO_OBJECT_ORG |
| `apo_minformula` | `string` | `varchar` |  |  | APO_MINFORMULA |
| `apo_maxformula` | `string` | `varchar` |  |  | APO_MAXFORMULA |
| `apo_confrating` | `string` | `varchar` |  |  | APO_CONFRATING |
| `apo_replacefreq` | `float` | `float` |  |  | APO_REPLACEFREQ |
| `apo_factor` | `float` | `float` |  |  | APO_FACTOR |
| `apo_risk` | `string` | `varchar` |  |  | APO_RISK |
| `apo_updated` | `timestamp` | `timestamp_ntz` |  |  | APO_UPDATED |
| `apo_updatecount` | `float` | `float` |  |  | APO_UPDATECOUNT |
| `apo_udfchar01` | `string` | `varchar` |  |  | APO_UDFCHAR01 |
| `apo_udfchar02` | `string` | `varchar` |  |  | APO_UDFCHAR02 |
| `apo_udfchar03` | `string` | `varchar` |  |  | APO_UDFCHAR03 |
| `apo_udfchar04` | `string` | `varchar` |  |  | APO_UDFCHAR04 |
| `apo_udfchar05` | `string` | `varchar` |  |  | APO_UDFCHAR05 |
| `apo_udfchar06` | `string` | `varchar` |  |  | APO_UDFCHAR06 |
| `apo_udfchar07` | `string` | `varchar` |  |  | APO_UDFCHAR07 |
| `apo_udfchar08` | `string` | `varchar` |  |  | APO_UDFCHAR08 |
| `apo_udfchar09` | `string` | `varchar` |  |  | APO_UDFCHAR09 |
| `apo_udfchar10` | `string` | `varchar` |  |  | APO_UDFCHAR10 |
| `apo_udfchar12` | `string` | `varchar` |  |  | APO_UDFCHAR12 |
| `apo_udfchar13` | `string` | `varchar` |  |  | APO_UDFCHAR13 |
| `apo_udfchar14` | `string` | `varchar` |  |  | APO_UDFCHAR14 |
| `apo_udfchar15` | `string` | `varchar` |  |  | APO_UDFCHAR15 |
| `apo_udfchar16` | `string` | `varchar` |  |  | APO_UDFCHAR16 |
| `apo_udfchar17` | `string` | `varchar` |  |  | APO_UDFCHAR17 |
| `apo_udfchar18` | `string` | `varchar` |  |  | APO_UDFCHAR18 |
| `apo_udfchar19` | `string` | `varchar` |  |  | APO_UDFCHAR19 |
| `apo_udfchar20` | `string` | `varchar` |  |  | APO_UDFCHAR20 |
| `apo_udfchar21` | `string` | `varchar` |  |  | APO_UDFCHAR21 |
| `apo_udfchar22` | `string` | `varchar` |  |  | APO_UDFCHAR22 |
| `apo_udfchar23` | `string` | `varchar` |  |  | APO_UDFCHAR23 |
| `apo_udfchar24` | `string` | `varchar` |  |  | APO_UDFCHAR24 |
| `apo_udfchar25` | `string` | `varchar` |  |  | APO_UDFCHAR25 |
| `apo_udfchar26` | `string` | `varchar` |  |  | APO_UDFCHAR26 |
| `apo_udfchar27` | `string` | `varchar` |  |  | APO_UDFCHAR27 |
| `apo_udfchar28` | `string` | `varchar` |  |  | APO_UDFCHAR28 |
| `apo_udfchar29` | `string` | `varchar` |  |  | APO_UDFCHAR29 |
| `apo_udfchar30` | `string` | `varchar` |  |  | APO_UDFCHAR30 |
| `apo_udfnum01` | `float` | `float` |  |  | APO_UDFNUM01 |
| `apo_udfnum02` | `float` | `float` |  |  | APO_UDFNUM02 |
| `apo_udfnum03` | `float` | `float` |  |  | APO_UDFNUM03 |
| `apo_udfnum04` | `float` | `float` |  |  | APO_UDFNUM04 |
| `apo_udfnum05` | `float` | `float` |  |  | APO_UDFNUM05 |
| `apo_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | APO_UDFDATE01 |
| `apo_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | APO_UDFDATE02 |
| `apo_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | APO_UDFDATE03 |
| `apo_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | APO_UDFDATE04 |
| `apo_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | APO_UDFDATE05 |
| `apo_udfchkbox01` | `string` | `varchar` |  |  | APO_UDFCHKBOX01 |
| `apo_udfchkbox02` | `string` | `varchar` |  |  | APO_UDFCHKBOX02 |
| `apo_udfchkbox03` | `string` | `varchar` |  |  | APO_UDFCHKBOX03 |
| `apo_udfchkbox04` | `string` | `varchar` |  |  | APO_UDFCHKBOX04 |
| `apo_udfchkbox05` | `string` | `varchar` |  |  | APO_UDFCHKBOX05 |
| `apo_udfchar11` | `string` | `varchar` |  |  | APO_UDFCHAR11 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

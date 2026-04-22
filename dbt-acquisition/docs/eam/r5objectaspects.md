# r5objectaspects

eam_R5OBJECTASPECTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5objectaspects` |
| **Materialization** | `incremental` |
| **Primary keys** | `oba_object`, `oba_object_org`, `oba_obtype`, `oba_aspect` |
| **Column count** | 77 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `oba_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | OBA_LASTSAVED |
| `oba_object` | `string` | `varchar` | `PK` |  | OBA_OBJECT |
| `oba_obrtype` | `string` | `varchar` |  |  | OBA_OBRTYPE |
| `oba_obtype` | `string` | `varchar` | `PK` |  | OBA_OBTYPE |
| `oba_aspect` | `string` | `varchar` | `PK` |  | OBA_ASPECT |
| `oba_object_org` | `string` | `varchar` | `PK` |  | OBA_OBJECT_ORG |
| `oba_risk` | `string` | `varchar` |  |  | OBA_RISK |
| `oba_method` | `string` | `varchar` |  |  | OBA_METHOD |
| `oba_confrating` | `string` | `varchar` |  |  | OBA_CONFRATING |
| `oba_nominal` | `float` | `float` |  |  | OBA_NOMINAL |
| `oba_uom` | `string` | `varchar` |  |  | OBA_UOM |
| `oba_minextr` | `float` | `float` |  |  | OBA_MINEXTR |
| `oba_min` | `float` | `float` |  |  | OBA_MIN |
| `oba_mintol` | `float` | `float` |  |  | OBA_MINTOL |
| `oba_minstwo` | `string` | `varchar` |  |  | OBA_MINSTWO |
| `oba_minppm` | `string` | `varchar` |  |  | OBA_MINPPM |
| `oba_minformula` | `string` | `varchar` |  |  | OBA_MINFORMULA |
| `oba_maxextr` | `float` | `float` |  |  | OBA_MAXEXTR |
| `oba_max` | `float` | `float` |  |  | OBA_MAX |
| `oba_maxtol` | `float` | `float` |  |  | OBA_MAXTOL |
| `oba_maxstwo` | `string` | `varchar` |  |  | OBA_MAXSTWO |
| `oba_maxppm` | `string` | `varchar` |  |  | OBA_MAXPPM |
| `oba_maxformula` | `string` | `varchar` |  |  | OBA_MAXFORMULA |
| `oba_replacefreq` | `float` | `float` |  |  | OBA_REPLACEFREQ |
| `oba_factor` | `float` | `float` |  |  | OBA_FACTOR |
| `oba_updatecount` | `float` | `float` |  |  | OBA_UPDATECOUNT |
| `oba_udfchar01` | `string` | `varchar` |  |  | OBA_UDFCHAR01 |
| `oba_udfchar02` | `string` | `varchar` |  |  | OBA_UDFCHAR02 |
| `oba_udfchar03` | `string` | `varchar` |  |  | OBA_UDFCHAR03 |
| `oba_udfchar04` | `string` | `varchar` |  |  | OBA_UDFCHAR04 |
| `oba_udfchar05` | `string` | `varchar` |  |  | OBA_UDFCHAR05 |
| `oba_udfchar06` | `string` | `varchar` |  |  | OBA_UDFCHAR06 |
| `oba_udfchar07` | `string` | `varchar` |  |  | OBA_UDFCHAR07 |
| `oba_udfchar08` | `string` | `varchar` |  |  | OBA_UDFCHAR08 |
| `oba_udfchar09` | `string` | `varchar` |  |  | OBA_UDFCHAR09 |
| `oba_udfchar10` | `string` | `varchar` |  |  | OBA_UDFCHAR10 |
| `oba_udfchar11` | `string` | `varchar` |  |  | OBA_UDFCHAR11 |
| `oba_udfchar12` | `string` | `varchar` |  |  | OBA_UDFCHAR12 |
| `oba_udfchar13` | `string` | `varchar` |  |  | OBA_UDFCHAR13 |
| `oba_udfchar14` | `string` | `varchar` |  |  | OBA_UDFCHAR14 |
| `oba_udfchar15` | `string` | `varchar` |  |  | OBA_UDFCHAR15 |
| `oba_udfchar16` | `string` | `varchar` |  |  | OBA_UDFCHAR16 |
| `oba_udfchar17` | `string` | `varchar` |  |  | OBA_UDFCHAR17 |
| `oba_udfchar18` | `string` | `varchar` |  |  | OBA_UDFCHAR18 |
| `oba_udfchar19` | `string` | `varchar` |  |  | OBA_UDFCHAR19 |
| `oba_udfchar20` | `string` | `varchar` |  |  | OBA_UDFCHAR20 |
| `oba_udfchar21` | `string` | `varchar` |  |  | OBA_UDFCHAR21 |
| `oba_udfchar22` | `string` | `varchar` |  |  | OBA_UDFCHAR22 |
| `oba_udfchar23` | `string` | `varchar` |  |  | OBA_UDFCHAR23 |
| `oba_udfchar24` | `string` | `varchar` |  |  | OBA_UDFCHAR24 |
| `oba_udfchar25` | `string` | `varchar` |  |  | OBA_UDFCHAR25 |
| `oba_udfchar26` | `string` | `varchar` |  |  | OBA_UDFCHAR26 |
| `oba_udfchar27` | `string` | `varchar` |  |  | OBA_UDFCHAR27 |
| `oba_udfchar28` | `string` | `varchar` |  |  | OBA_UDFCHAR28 |
| `oba_udfchar29` | `string` | `varchar` |  |  | OBA_UDFCHAR29 |
| `oba_udfchar30` | `string` | `varchar` |  |  | OBA_UDFCHAR30 |
| `oba_udfnum01` | `float` | `float` |  |  | OBA_UDFNUM01 |
| `oba_udfnum02` | `float` | `float` |  |  | OBA_UDFNUM02 |
| `oba_udfnum03` | `float` | `float` |  |  | OBA_UDFNUM03 |
| `oba_udfnum04` | `float` | `float` |  |  | OBA_UDFNUM04 |
| `oba_udfnum05` | `float` | `float` |  |  | OBA_UDFNUM05 |
| `oba_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | OBA_UDFDATE01 |
| `oba_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | OBA_UDFDATE02 |
| `oba_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | OBA_UDFDATE03 |
| `oba_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | OBA_UDFDATE04 |
| `oba_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | OBA_UDFDATE05 |
| `oba_udfchkbox01` | `string` | `varchar` |  |  | OBA_UDFCHKBOX01 |
| `oba_udfchkbox02` | `string` | `varchar` |  |  | OBA_UDFCHKBOX02 |
| `oba_udfchkbox03` | `string` | `varchar` |  |  | OBA_UDFCHKBOX03 |
| `oba_udfchkbox04` | `string` | `varchar` |  |  | OBA_UDFCHKBOX04 |
| `oba_udfchkbox05` | `string` | `varchar` |  |  | OBA_UDFCHKBOX05 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

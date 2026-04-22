# r5ppmacts

eam_R5PPMACTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5ppmacts` |
| **Materialization** | `incremental` |
| **Primary keys** | `ppa_ppm`, `ppa_revision`, `ppa_act` |
| **Column count** | 86 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ppa_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | PPA_LASTSAVED |
| `ppa_ppm` | `string` | `varchar` | `PK` |  | PPA_PPM |
| `ppa_act` | `float` | `float` | `PK` |  | PPA_ACT |
| `ppa_trade` | `string` | `varchar` |  |  | PPA_TRADE |
| `ppa_duration` | `float` | `float` |  |  | PPA_DURATION |
| `ppa_est` | `float` | `float` |  |  | PPA_EST |
| `ppa_task` | `string` | `varchar` |  |  | PPA_TASK |
| `ppa_matlist` | `string` | `varchar` |  |  | PPA_MATLIST |
| `ppa_persons` | `float` | `float` |  |  | PPA_PERSONS |
| `ppa_start` | `float` | `float` |  |  | PPA_START |
| `ppa_special` | `string` | `varchar` |  |  | PPA_SPECIAL |
| `ppa_hire` | `string` | `varchar` |  |  | PPA_HIRE |
| `ppa_revision` | `float` | `float` | `PK` |  | PPA_REVISION |
| `ppa_qty` | `float` | `float` |  |  | PPA_QTY |
| `ppa_uom` | `string` | `varchar` |  |  | PPA_UOM |
| `ppa_how` | `string` | `varchar` |  |  | PPA_HOW |
| `ppa_action` | `string` | `varchar` |  |  | PPA_ACTION |
| `ppa_what` | `string` | `varchar` |  |  | PPA_WHAT |
| `ppa_where` | `string` | `varchar` |  |  | PPA_WHERE |
| `ppa_condition` | `string` | `varchar` |  |  | PPA_CONDITION |
| `ppa_rpc` | `string` | `varchar` |  |  | PPA_RPC |
| `ppa_wap` | `string` | `varchar` |  |  | PPA_WAP |
| `ppa_tpf` | `string` | `varchar` |  |  | PPA_TPF |
| `ppa_manufact` | `string` | `varchar` |  |  | PPA_MANUFACT |
| `ppa_syslevel` | `string` | `varchar` |  |  | PPA_SYSLEVEL |
| `ppa_asmlevel` | `string` | `varchar` |  |  | PPA_ASMLEVEL |
| `ppa_complevel` | `string` | `varchar` |  |  | PPA_COMPLEVEL |
| `ppa_updatecount` | `float` | `float` |  |  | PPA_UPDATECOUNT |
| `ppa_note` | `string` | `varchar` |  |  | PPA_NOTE |
| `ppa_udfchar01` | `string` | `varchar` |  |  | PPA_UDFCHAR01 |
| `ppa_udfchar02` | `string` | `varchar` |  |  | PPA_UDFCHAR02 |
| `ppa_udfchar03` | `string` | `varchar` |  |  | PPA_UDFCHAR03 |
| `ppa_udfchar04` | `string` | `varchar` |  |  | PPA_UDFCHAR04 |
| `ppa_udfchar05` | `string` | `varchar` |  |  | PPA_UDFCHAR05 |
| `ppa_udfchar06` | `string` | `varchar` |  |  | PPA_UDFCHAR06 |
| `ppa_udfchar07` | `string` | `varchar` |  |  | PPA_UDFCHAR07 |
| `ppa_udfchar08` | `string` | `varchar` |  |  | PPA_UDFCHAR08 |
| `ppa_udfchar09` | `string` | `varchar` |  |  | PPA_UDFCHAR09 |
| `ppa_udfchar10` | `string` | `varchar` |  |  | PPA_UDFCHAR10 |
| `ppa_udfchar11` | `string` | `varchar` |  |  | PPA_UDFCHAR11 |
| `ppa_udfchar12` | `string` | `varchar` |  |  | PPA_UDFCHAR12 |
| `ppa_udfchar13` | `string` | `varchar` |  |  | PPA_UDFCHAR13 |
| `ppa_udfchar14` | `string` | `varchar` |  |  | PPA_UDFCHAR14 |
| `ppa_udfchar15` | `string` | `varchar` |  |  | PPA_UDFCHAR15 |
| `ppa_udfchar16` | `string` | `varchar` |  |  | PPA_UDFCHAR16 |
| `ppa_udfchar17` | `string` | `varchar` |  |  | PPA_UDFCHAR17 |
| `ppa_udfchar18` | `string` | `varchar` |  |  | PPA_UDFCHAR18 |
| `ppa_udfchar19` | `string` | `varchar` |  |  | PPA_UDFCHAR19 |
| `ppa_udfchar20` | `string` | `varchar` |  |  | PPA_UDFCHAR20 |
| `ppa_udfchar21` | `string` | `varchar` |  |  | PPA_UDFCHAR21 |
| `ppa_udfchar22` | `string` | `varchar` |  |  | PPA_UDFCHAR22 |
| `ppa_udfchar23` | `string` | `varchar` |  |  | PPA_UDFCHAR23 |
| `ppa_udfchar24` | `string` | `varchar` |  |  | PPA_UDFCHAR24 |
| `ppa_udfchar25` | `string` | `varchar` |  |  | PPA_UDFCHAR25 |
| `ppa_udfchar26` | `string` | `varchar` |  |  | PPA_UDFCHAR26 |
| `ppa_udfchar27` | `string` | `varchar` |  |  | PPA_UDFCHAR27 |
| `ppa_udfchar28` | `string` | `varchar` |  |  | PPA_UDFCHAR28 |
| `ppa_udfchar29` | `string` | `varchar` |  |  | PPA_UDFCHAR29 |
| `ppa_udfchar30` | `string` | `varchar` |  |  | PPA_UDFCHAR30 |
| `ppa_udfnum01` | `float` | `float` |  |  | PPA_UDFNUM01 |
| `ppa_udfnum02` | `float` | `float` |  |  | PPA_UDFNUM02 |
| `ppa_udfnum03` | `float` | `float` |  |  | PPA_UDFNUM03 |
| `ppa_udfnum04` | `float` | `float` |  |  | PPA_UDFNUM04 |
| `ppa_udfnum05` | `float` | `float` |  |  | PPA_UDFNUM05 |
| `ppa_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | PPA_UDFDATE01 |
| `ppa_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | PPA_UDFDATE02 |
| `ppa_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | PPA_UDFDATE03 |
| `ppa_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | PPA_UDFDATE04 |
| `ppa_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | PPA_UDFDATE05 |
| `ppa_udfchkbox01` | `string` | `varchar` |  |  | PPA_UDFCHKBOX01 |
| `ppa_udfchkbox02` | `string` | `varchar` |  |  | PPA_UDFCHKBOX02 |
| `ppa_udfchkbox03` | `string` | `varchar` |  |  | PPA_UDFCHKBOX03 |
| `ppa_udfchkbox04` | `string` | `varchar` |  |  | PPA_UDFCHKBOX04 |
| `ppa_udfchkbox05` | `string` | `varchar` |  |  | PPA_UDFCHKBOX05 |
| `ppa_udfnote01` | `string` | `varchar` |  |  | PPA_UDFNOTE01 |
| `ppa_udfnote02` | `string` | `varchar` |  |  | PPA_UDFNOTE02 |
| `ppa_supplier` | `string` | `varchar` |  |  | PPA_SUPPLIER |
| `ppa_labortype` | `string` | `varchar` |  |  | PPA_LABORTYPE |
| `ppa_laborrtype` | `string` | `varchar` |  |  | PPA_LABORRTYPE |
| `ppa_supplier_org` | `string` | `varchar` |  |  | PPA_SUPPLIER_ORG |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# r5standwacts

eam_R5STANDWACTS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5standwacts` |
| **Materialization** | `incremental` |
| **Primary keys** | `wac_standwork`, `wac_act` |
| **Column count** | 82 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `wac_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | WAC_LASTSAVED |
| `wac_trade` | `string` | `varchar` |  |  | WAC_TRADE |
| `wac_duration` | `float` | `float` |  |  | WAC_DURATION |
| `wac_est` | `float` | `float` |  |  | WAC_EST |
| `wac_task` | `string` | `varchar` |  |  | WAC_TASK |
| `wac_matlist` | `string` | `varchar` |  |  | WAC_MATLIST |
| `wac_special` | `string` | `varchar` |  |  | WAC_SPECIAL |
| `wac_hire` | `string` | `varchar` |  |  | WAC_HIRE |
| `wac_persons` | `float` | `float` |  |  | WAC_PERSONS |
| `wac_start` | `float` | `float` |  |  | WAC_START |
| `wac_qty` | `float` | `float` |  |  | WAC_QTY |
| `wac_uom` | `string` | `varchar` |  |  | WAC_UOM |
| `wac_rpc` | `string` | `varchar` |  |  | WAC_RPC |
| `wac_wap` | `string` | `varchar` |  |  | WAC_WAP |
| `wac_tpf` | `string` | `varchar` |  |  | WAC_TPF |
| `wac_manufact` | `string` | `varchar` |  |  | WAC_MANUFACT |
| `wac_syslevel` | `string` | `varchar` |  |  | WAC_SYSLEVEL |
| `wac_asmlevel` | `string` | `varchar` |  |  | WAC_ASMLEVEL |
| `wac_complevel` | `string` | `varchar` |  |  | WAC_COMPLEVEL |
| `wac_updatecount` | `float` | `float` |  |  | WAC_UPDATECOUNT |
| `wac_created` | `timestamp` | `timestamp_ntz` |  |  | WAC_CREATED |
| `wac_updated` | `timestamp` | `timestamp_ntz` |  |  | WAC_UPDATED |
| `wac_note` | `string` | `varchar` |  |  | WAC_NOTE |
| `wac_udfchar01` | `string` | `varchar` |  |  | WAC_UDFCHAR01 |
| `wac_udfchar02` | `string` | `varchar` |  |  | WAC_UDFCHAR02 |
| `wac_udfchar03` | `string` | `varchar` |  |  | WAC_UDFCHAR03 |
| `wac_udfchar04` | `string` | `varchar` |  |  | WAC_UDFCHAR04 |
| `wac_udfchar05` | `string` | `varchar` |  |  | WAC_UDFCHAR05 |
| `wac_udfchar06` | `string` | `varchar` |  |  | WAC_UDFCHAR06 |
| `wac_udfchar07` | `string` | `varchar` |  |  | WAC_UDFCHAR07 |
| `wac_udfchar08` | `string` | `varchar` |  |  | WAC_UDFCHAR08 |
| `wac_udfchar09` | `string` | `varchar` |  |  | WAC_UDFCHAR09 |
| `wac_udfchar10` | `string` | `varchar` |  |  | WAC_UDFCHAR10 |
| `wac_udfchar11` | `string` | `varchar` |  |  | WAC_UDFCHAR11 |
| `wac_udfchar12` | `string` | `varchar` |  |  | WAC_UDFCHAR12 |
| `wac_udfchar13` | `string` | `varchar` |  |  | WAC_UDFCHAR13 |
| `wac_udfchar14` | `string` | `varchar` |  |  | WAC_UDFCHAR14 |
| `wac_udfchar15` | `string` | `varchar` |  |  | WAC_UDFCHAR15 |
| `wac_udfchar16` | `string` | `varchar` |  |  | WAC_UDFCHAR16 |
| `wac_udfchar17` | `string` | `varchar` |  |  | WAC_UDFCHAR17 |
| `wac_udfchar18` | `string` | `varchar` |  |  | WAC_UDFCHAR18 |
| `wac_udfchar19` | `string` | `varchar` |  |  | WAC_UDFCHAR19 |
| `wac_udfchar20` | `string` | `varchar` |  |  | WAC_UDFCHAR20 |
| `wac_udfchar21` | `string` | `varchar` |  |  | WAC_UDFCHAR21 |
| `wac_udfchar22` | `string` | `varchar` |  |  | WAC_UDFCHAR22 |
| `wac_udfchar23` | `string` | `varchar` |  |  | WAC_UDFCHAR23 |
| `wac_udfchar24` | `string` | `varchar` |  |  | WAC_UDFCHAR24 |
| `wac_udfchar25` | `string` | `varchar` |  |  | WAC_UDFCHAR25 |
| `wac_udfchar26` | `string` | `varchar` |  |  | WAC_UDFCHAR26 |
| `wac_udfchar27` | `string` | `varchar` |  |  | WAC_UDFCHAR27 |
| `wac_udfchar28` | `string` | `varchar` |  |  | WAC_UDFCHAR28 |
| `wac_udfchar29` | `string` | `varchar` |  |  | WAC_UDFCHAR29 |
| `wac_udfchar30` | `string` | `varchar` |  |  | WAC_UDFCHAR30 |
| `wac_udfnum01` | `float` | `float` |  |  | WAC_UDFNUM01 |
| `wac_udfnum02` | `float` | `float` |  |  | WAC_UDFNUM02 |
| `wac_udfnum03` | `float` | `float` |  |  | WAC_UDFNUM03 |
| `wac_udfnum04` | `float` | `float` |  |  | WAC_UDFNUM04 |
| `wac_udfnum05` | `float` | `float` |  |  | WAC_UDFNUM05 |
| `wac_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | WAC_UDFDATE01 |
| `wac_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | WAC_UDFDATE02 |
| `wac_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | WAC_UDFDATE03 |
| `wac_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | WAC_UDFDATE04 |
| `wac_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | WAC_UDFDATE05 |
| `wac_udfchkbox01` | `string` | `varchar` |  |  | WAC_UDFCHKBOX01 |
| `wac_udfchkbox02` | `string` | `varchar` |  |  | WAC_UDFCHKBOX02 |
| `wac_udfchkbox03` | `string` | `varchar` |  |  | WAC_UDFCHKBOX03 |
| `wac_udfchkbox04` | `string` | `varchar` |  |  | WAC_UDFCHKBOX04 |
| `wac_udfchkbox05` | `string` | `varchar` |  |  | WAC_UDFCHKBOX05 |
| `wac_udfnote01` | `string` | `varchar` |  |  | WAC_UDFNOTE01 |
| `wac_udfnote02` | `string` | `varchar` |  |  | WAC_UDFNOTE02 |
| `wac_labortype` | `string` | `varchar` |  |  | WAC_LABORTYPE |
| `wac_laborrtype` | `string` | `varchar` |  |  | WAC_LABORRTYPE |
| `wac_supplier` | `string` | `varchar` |  |  | WAC_SUPPLIER |
| `wac_supplier_org` | `string` | `varchar` |  |  | WAC_SUPPLIER_ORG |
| `wac_standwork` | `string` | `varchar` | `PK` |  | WAC_STANDWORK |
| `wac_act` | `float` | `float` | `PK` |  | WAC_ACT |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

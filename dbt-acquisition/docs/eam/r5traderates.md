# r5traderates

eam_R5TRADERATES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5traderates` |
| **Materialization** | `incremental` |
| **Primary keys** | `trr_mrc`, `trr_trade`, `trr_supplier`, `trr_supplier_org`, `trr_person`, `trr_org`, `trr_start`, `trr_octype` |
| **Column count** | 67 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `trr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | TRR_LASTSAVED |
| `trr_udfchar16` | `string` | `varchar` |  |  | TRR_UDFCHAR16 |
| `trr_udfchar19` | `string` | `varchar` |  |  | TRR_UDFCHAR19 |
| `trr_udfchar20` | `string` | `varchar` |  |  | TRR_UDFCHAR20 |
| `trr_udfchar21` | `string` | `varchar` |  |  | TRR_UDFCHAR21 |
| `trr_udfchar22` | `string` | `varchar` |  |  | TRR_UDFCHAR22 |
| `trr_udfchar23` | `string` | `varchar` |  |  | TRR_UDFCHAR23 |
| `trr_udfchar24` | `string` | `varchar` |  |  | TRR_UDFCHAR24 |
| `trr_udfchar25` | `string` | `varchar` |  |  | TRR_UDFCHAR25 |
| `trr_udfchar26` | `string` | `varchar` |  |  | TRR_UDFCHAR26 |
| `trr_udfchar27` | `string` | `varchar` |  |  | TRR_UDFCHAR27 |
| `trr_udfchar28` | `string` | `varchar` |  |  | TRR_UDFCHAR28 |
| `trr_udfchar29` | `string` | `varchar` |  |  | TRR_UDFCHAR29 |
| `trr_udfchar30` | `string` | `varchar` |  |  | TRR_UDFCHAR30 |
| `trr_udfnum01` | `float` | `float` |  |  | TRR_UDFNUM01 |
| `trr_udfnum02` | `float` | `float` |  |  | TRR_UDFNUM02 |
| `trr_udfnum03` | `float` | `float` |  |  | TRR_UDFNUM03 |
| `trr_udfnum04` | `float` | `float` |  |  | TRR_UDFNUM04 |
| `trr_udfnum05` | `float` | `float` |  |  | TRR_UDFNUM05 |
| `trr_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | TRR_UDFDATE01 |
| `trr_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | TRR_UDFDATE02 |
| `trr_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | TRR_UDFDATE03 |
| `trr_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | TRR_UDFDATE04 |
| `trr_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | TRR_UDFDATE05 |
| `trr_udfchkbox01` | `string` | `varchar` |  |  | TRR_UDFCHKBOX01 |
| `trr_udfchkbox02` | `string` | `varchar` |  |  | TRR_UDFCHKBOX02 |
| `trr_udfchkbox03` | `string` | `varchar` |  |  | TRR_UDFCHKBOX03 |
| `trr_udfchkbox04` | `string` | `varchar` |  |  | TRR_UDFCHKBOX04 |
| `trr_udfchkbox05` | `string` | `varchar` |  |  | TRR_UDFCHKBOX05 |
| `trr_custandardrate` | `float` | `float` |  |  | TRR_CUSTANDARDRATE |
| `trr_mrc` | `string` | `varchar` | `PK` |  | TRR_MRC |
| `trr_trade` | `string` | `varchar` | `PK` |  | TRR_TRADE |
| `trr_supplier` | `string` | `varchar` | `PK` |  | TRR_SUPPLIER |
| `trr_start` | `timestamp` | `timestamp_ntz` | `PK` |  | TRR_START |
| `trr_end` | `timestamp` | `timestamp_ntz` |  |  | TRR_END |
| `trr_ntrate` | `float` | `float` |  |  | TRR_NTRATE |
| `trr_otrate` | `float` | `float` |  |  | TRR_OTRATE |
| `trr_active` | `string` | `varchar` |  |  | TRR_ACTIVE |
| `trr_octype` | `string` | `varchar` | `PK` |  | TRR_OCTYPE |
| `trr_supplier_org` | `string` | `varchar` | `PK` |  | TRR_SUPPLIER_ORG |
| `trr_org` | `string` | `varchar` | `PK` |  | TRR_ORG |
| `trr_updatecount` | `float` | `float` |  |  | TRR_UPDATECOUNT |
| `trr_person` | `string` | `varchar` | `PK` |  | TRR_PERSON |
| `trr_tax` | `string` | `varchar` |  |  | TRR_TAX |
| `trr_udfchar01` | `string` | `varchar` |  |  | TRR_UDFCHAR01 |
| `trr_udfchar02` | `string` | `varchar` |  |  | TRR_UDFCHAR02 |
| `trr_udfchar03` | `string` | `varchar` |  |  | TRR_UDFCHAR03 |
| `trr_udfchar04` | `string` | `varchar` |  |  | TRR_UDFCHAR04 |
| `trr_udfchar05` | `string` | `varchar` |  |  | TRR_UDFCHAR05 |
| `trr_udfchar06` | `string` | `varchar` |  |  | TRR_UDFCHAR06 |
| `trr_udfchar07` | `string` | `varchar` |  |  | TRR_UDFCHAR07 |
| `trr_udfchar08` | `string` | `varchar` |  |  | TRR_UDFCHAR08 |
| `trr_udfchar09` | `string` | `varchar` |  |  | TRR_UDFCHAR09 |
| `trr_udfchar10` | `string` | `varchar` |  |  | TRR_UDFCHAR10 |
| `trr_udfchar11` | `string` | `varchar` |  |  | TRR_UDFCHAR11 |
| `trr_udfchar12` | `string` | `varchar` |  |  | TRR_UDFCHAR12 |
| `trr_udfchar13` | `string` | `varchar` |  |  | TRR_UDFCHAR13 |
| `trr_udfchar14` | `string` | `varchar` |  |  | TRR_UDFCHAR14 |
| `trr_udfchar15` | `string` | `varchar` |  |  | TRR_UDFCHAR15 |
| `trr_udfchar17` | `string` | `varchar` |  |  | TRR_UDFCHAR17 |
| `trr_udfchar18` | `string` | `varchar` |  |  | TRR_UDFCHAR18 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

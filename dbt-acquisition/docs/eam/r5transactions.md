# r5transactions

eam_R5TRANSACTIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5transactions` |
| **Materialization** | `incremental` |
| **Primary keys** | `tra_code` |
| **Column count** | 98 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `tra_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | TRA_LASTSAVED |
| `tra_code` | `string` | `varchar` | `PK` | `unique` | TRA_CODE |
| `tra_desc` | `string` | `varchar` |  |  | TRA_DESC |
| `tra_class` | `string` | `varchar` |  |  | TRA_CLASS |
| `tra_type` | `string` | `varchar` |  |  | TRA_TYPE |
| `tra_rtype` | `string` | `varchar` |  |  | TRA_RTYPE |
| `tra_advice` | `string` | `varchar` |  |  | TRA_ADVICE |
| `tra_auth` | `string` | `varchar` |  |  | TRA_AUTH |
| `tra_date` | `timestamp` | `timestamp_ntz` |  |  | TRA_DATE |
| `tra_order` | `string` | `varchar` |  |  | TRA_ORDER |
| `tra_pridest` | `string` | `varchar` |  |  | TRA_PRIDEST |
| `tra_priorig` | `string` | `varchar` |  |  | TRA_PRIORIG |
| `tra_req` | `string` | `varchar` |  |  | TRA_REQ |
| `tra_status` | `string` | `varchar` |  |  | TRA_STATUS |
| `tra_rstatus` | `string` | `varchar` |  |  | TRA_RSTATUS |
| `tra_fromentity` | `string` | `varchar` |  |  | TRA_FROMENTITY |
| `tra_fromrentity` | `string` | `varchar` |  |  | TRA_FROMRENTITY |
| `tra_fromtype` | `string` | `varchar` |  |  | TRA_FROMTYPE |
| `tra_fromrtype` | `string` | `varchar` |  |  | TRA_FROMRTYPE |
| `tra_fromcode` | `string` | `varchar` |  |  | TRA_FROMCODE |
| `tra_toentity` | `string` | `varchar` |  |  | TRA_TOENTITY |
| `tra_torentity` | `string` | `varchar` |  |  | TRA_TORENTITY |
| `tra_totype` | `string` | `varchar` |  |  | TRA_TOTYPE |
| `tra_tortype` | `string` | `varchar` |  |  | TRA_TORTYPE |
| `tra_tocode` | `string` | `varchar` |  |  | TRA_TOCODE |
| `tra_printed` | `string` | `varchar` |  |  | TRA_PRINTED |
| `tra_sourcesystem` | `string` | `varchar` |  |  | TRA_SOURCESYSTEM |
| `tra_sourcecode` | `string` | `varchar` |  |  | TRA_SOURCECODE |
| `tra_interface` | `timestamp` | `timestamp_ntz` |  |  | TRA_INTERFACE |
| `tra_acd` | `float` | `float` |  |  | TRA_ACD |
| `tra_org` | `string` | `varchar` |  |  | TRA_ORG |
| `tra_class_org` | `string` | `varchar` |  |  | TRA_CLASS_ORG |
| `tra_tocode_org` | `string` | `varchar` |  |  | TRA_TOCODE_ORG |
| `tra_fromcode_org` | `string` | `varchar` |  |  | TRA_FROMCODE_ORG |
| `tra_order_org` | `string` | `varchar` |  |  | TRA_ORDER_ORG |
| `tra_dckcode` | `string` | `varchar` |  |  | TRA_DCKCODE |
| `tra_dckline` | `float` | `float` |  |  | TRA_DCKLINE |
| `tra_pers` | `string` | `varchar` |  |  | TRA_PERS |
| `tra_iptransmitted` | `string` | `varchar` |  |  | TRA_IPTRANSMITTED |
| `tra_updatecount` | `float` | `float` |  |  | TRA_UPDATECOUNT |
| `tra_created` | `timestamp` | `timestamp_ntz` |  |  | TRA_CREATED |
| `tra_updated` | `timestamp` | `timestamp_ntz` |  |  | TRA_UPDATED |
| `tra_jecategory` | `string` | `varchar` |  |  | TRA_JECATEGORY |
| `tra_jesource` | `string` | `varchar` |  |  | TRA_JESOURCE |
| `tra_routeparent` | `string` | `varchar` |  |  | TRA_ROUTEPARENT |
| `tra_invallocation` | `float` | `float` |  |  | TRA_INVALLOCATION |
| `tra_relatedwo` | `string` | `varchar` |  |  | TRA_RELATEDWO |
| `tra_udfchar01` | `string` | `varchar` |  |  | TRA_UDFCHAR01 |
| `tra_udfchar02` | `string` | `varchar` |  |  | TRA_UDFCHAR02 |
| `tra_udfchar03` | `string` | `varchar` |  |  | TRA_UDFCHAR03 |
| `tra_udfchar04` | `string` | `varchar` |  |  | TRA_UDFCHAR04 |
| `tra_udfchar05` | `string` | `varchar` |  |  | TRA_UDFCHAR05 |
| `tra_udfchar06` | `string` | `varchar` |  |  | TRA_UDFCHAR06 |
| `tra_udfchar07` | `string` | `varchar` |  |  | TRA_UDFCHAR07 |
| `tra_udfchar08` | `string` | `varchar` |  |  | TRA_UDFCHAR08 |
| `tra_udfchar09` | `string` | `varchar` |  |  | TRA_UDFCHAR09 |
| `tra_udfchar10` | `string` | `varchar` |  |  | TRA_UDFCHAR10 |
| `tra_udfchar11` | `string` | `varchar` |  |  | TRA_UDFCHAR11 |
| `tra_udfchar12` | `string` | `varchar` |  |  | TRA_UDFCHAR12 |
| `tra_udfchar13` | `string` | `varchar` |  |  | TRA_UDFCHAR13 |
| `tra_udfchar14` | `string` | `varchar` |  |  | TRA_UDFCHAR14 |
| `tra_udfchar15` | `string` | `varchar` |  |  | TRA_UDFCHAR15 |
| `tra_udfchar16` | `string` | `varchar` |  |  | TRA_UDFCHAR16 |
| `tra_udfchar17` | `string` | `varchar` |  |  | TRA_UDFCHAR17 |
| `tra_udfchar18` | `string` | `varchar` |  |  | TRA_UDFCHAR18 |
| `tra_udfchar22` | `string` | `varchar` |  |  | TRA_UDFCHAR22 |
| `tra_udfchar19` | `string` | `varchar` |  |  | TRA_UDFCHAR19 |
| `tra_udfchar20` | `string` | `varchar` |  |  | TRA_UDFCHAR20 |
| `tra_udfchar21` | `string` | `varchar` |  |  | TRA_UDFCHAR21 |
| `tra_udfchar23` | `string` | `varchar` |  |  | TRA_UDFCHAR23 |
| `tra_udfchar24` | `string` | `varchar` |  |  | TRA_UDFCHAR24 |
| `tra_udfchar25` | `string` | `varchar` |  |  | TRA_UDFCHAR25 |
| `tra_udfchar26` | `string` | `varchar` |  |  | TRA_UDFCHAR26 |
| `tra_udfchar27` | `string` | `varchar` |  |  | TRA_UDFCHAR27 |
| `tra_udfchar28` | `string` | `varchar` |  |  | TRA_UDFCHAR28 |
| `tra_udfchar29` | `string` | `varchar` |  |  | TRA_UDFCHAR29 |
| `tra_udfchar30` | `string` | `varchar` |  |  | TRA_UDFCHAR30 |
| `tra_udfnum01` | `float` | `float` |  |  | TRA_UDFNUM01 |
| `tra_udfnum02` | `float` | `float` |  |  | TRA_UDFNUM02 |
| `tra_udfnum03` | `float` | `float` |  |  | TRA_UDFNUM03 |
| `tra_udfnum04` | `float` | `float` |  |  | TRA_UDFNUM04 |
| `tra_udfnum05` | `float` | `float` |  |  | TRA_UDFNUM05 |
| `tra_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | TRA_UDFDATE01 |
| `tra_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | TRA_UDFDATE02 |
| `tra_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | TRA_UDFDATE03 |
| `tra_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | TRA_UDFDATE04 |
| `tra_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | TRA_UDFDATE05 |
| `tra_udfchkbox01` | `string` | `varchar` |  |  | TRA_UDFCHKBOX01 |
| `tra_udfchkbox02` | `string` | `varchar` |  |  | TRA_UDFCHKBOX02 |
| `tra_udfchkbox03` | `string` | `varchar` |  |  | TRA_UDFCHKBOX03 |
| `tra_udfchkbox04` | `string` | `varchar` |  |  | TRA_UDFCHKBOX04 |
| `tra_udfchkbox05` | `string` | `varchar` |  |  | TRA_UDFCHKBOX05 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

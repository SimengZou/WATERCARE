# r5stock

eam_R5STOCK

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5stock` |
| **Materialization** | `incremental` |
| **Primary keys** | `sto_part`, `sto_part_org`, `sto_store` |
| **Column count** | 122 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `sto_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | STO_LASTSAVED |
| `sto_part` | `string` | `varchar` | `PK` |  | STO_PART |
| `sto_store` | `string` | `varchar` | `PK` |  | STO_STORE |
| `sto_forinv` | `float` | `float` |  |  | STO_FORINV |
| `sto_minlev` | `float` | `float` |  |  | STO_MINLEV |
| `sto_ordlev` | `float` | `float` |  |  | STO_ORDLEV |
| `sto_ordqty` | `float` | `float` |  |  | STO_ORDQTY |
| `sto_stocktake` | `timestamp` | `timestamp_ntz` |  |  | STO_STOCKTAKE |
| `sto_prefstore` | `string` | `varchar` |  |  | STO_PREFSTORE |
| `sto_prefprice` | `float` | `float` |  |  | STO_PREFPRICE |
| `sto_prefsup` | `string` | `varchar` |  |  | STO_PREFSUP |
| `sto_prefuom` | `string` | `varchar` |  |  | STO_PREFUOM |
| `sto_leadtime` | `float` | `float` |  |  | STO_LEADTIME |
| `sto_class` | `string` | `varchar` |  |  | STO_CLASS |
| `sto_qty` | `float` | `float` |  |  | STO_QTY |
| `sto_avgprice` | `float` | `float` |  |  | STO_AVGPRICE |
| `sto_lastprice` | `float` | `float` |  |  | STO_LASTPRICE |
| `sto_stdprice` | `float` | `float` |  |  | STO_STDPRICE |
| `sto_baseprice` | `float` | `float` |  |  | STO_BASEPRICE |
| `sto_stockout` | `string` | `varchar` |  |  | STO_STOCKOUT |
| `sto_consignment` | `string` | `varchar` |  |  | STO_CONSIGNMENT |
| `sto_abcclass` | `string` | `varchar` |  |  | STO_ABCCLASS |
| `sto_class_org` | `string` | `varchar` |  |  | STO_CLASS_ORG |
| `sto_prefsup_org` | `string` | `varchar` |  |  | STO_PREFSUP_ORG |
| `sto_part_org` | `string` | `varchar` | `PK` |  | STO_PART_ORG |
| `sto_pricetype` | `string` | `varchar` |  |  | STO_PRICETYPE |
| `sto_credit` | `float` | `float` |  |  | STO_CREDIT |
| `sto_ondemand` | `string` | `varchar` |  |  | STO_ONDEMAND |
| `sto_corevalue` | `float` | `float` |  |  | STO_COREVALUE |
| `sto_repairqty` | `float` | `float` |  |  | STO_REPAIRQTY |
| `sto_vendorqty` | `float` | `float` |  |  | STO_VENDORQTY |
| `sto_shopqty` | `float` | `float` |  |  | STO_SHOPQTY |
| `sto_repminqty` | `float` | `float` |  |  | STO_REPMINQTY |
| `sto_repprice` | `float` | `float` |  |  | STO_REPPRICE |
| `sto_repleadtime` | `float` | `float` |  |  | STO_REPLEADTIME |
| `sto_repprefsup` | `string` | `varchar` |  |  | STO_REPPREFSUP |
| `sto_repprefsup_org` | `string` | `varchar` |  |  | STO_REPPREFSUP_ORG |
| `sto_repmrc` | `string` | `varchar` |  |  | STO_REPMRC |
| `sto_repstandwork` | `string` | `varchar` |  |  | STO_REPSTANDWORK |
| `sto_repobject` | `string` | `varchar` |  |  | STO_REPOBJECT |
| `sto_repobject_org` | `string` | `varchar` |  |  | STO_REPOBJECT_ORG |
| `sto_repairtype` | `string` | `varchar` |  |  | STO_REPAIRTYPE |
| `sto_defaultbin` | `string` | `varchar` |  |  | STO_DEFAULTBIN |
| `sto_updatecount` | `float` | `float` |  |  | STO_UPDATECOUNT |
| `sto_created` | `timestamp` | `timestamp_ntz` |  |  | STO_CREATED |
| `sto_updated` | `timestamp` | `timestamp_ntz` |  |  | STO_UPDATED |
| `sto_maxqty` | `float` | `float` |  |  | STO_MAXQTY |
| `sto_repdefaultbin` | `string` | `varchar` |  |  | STO_REPDEFAULTBIN |
| `sto_repstockmethod` | `string` | `varchar` |  |  | STO_REPSTOCKMETHOD |
| `sto_repautoassign` | `string` | `varchar` |  |  | STO_REPAUTOASSIGN |
| `sto_printer` | `string` | `varchar` |  |  | STO_PRINTER |
| `sto_issuetemplate` | `string` | `varchar` |  |  | STO_ISSUETEMPLATE |
| `sto_receipttemplate` | `string` | `varchar` |  |  | STO_RECEIPTTEMPLATE |
| `sto_nonpotemplate` | `string` | `varchar` |  |  | STO_NONPOTEMPLATE |
| `sto_labeldefault` | `string` | `varchar` |  |  | STO_LABELDEFAULT |
| `sto_prefmanufacturer` | `string` | `varchar` |  |  | STO_PREFMANUFACTURER |
| `sto_prefmanufactpart` | `string` | `varchar` |  |  | STO_PREFMANUFACTPART |
| `sto_streceipttemplate` | `string` | `varchar` |  |  | STO_STRECEIPTTEMPLATE |
| `sto_udfchar01` | `string` | `varchar` |  |  | STO_UDFCHAR01 |
| `sto_udfchar02` | `string` | `varchar` |  |  | STO_UDFCHAR02 |
| `sto_udfchar03` | `string` | `varchar` |  |  | STO_UDFCHAR03 |
| `sto_udfchar04` | `string` | `varchar` |  |  | STO_UDFCHAR04 |
| `sto_udfchar05` | `string` | `varchar` |  |  | STO_UDFCHAR05 |
| `sto_udfchar06` | `string` | `varchar` |  |  | STO_UDFCHAR06 |
| `sto_udfchar07` | `string` | `varchar` |  |  | STO_UDFCHAR07 |
| `sto_udfchar08` | `string` | `varchar` |  |  | STO_UDFCHAR08 |
| `sto_udfchar09` | `string` | `varchar` |  |  | STO_UDFCHAR09 |
| `sto_udfchar10` | `string` | `varchar` |  |  | STO_UDFCHAR10 |
| `sto_udfchar11` | `string` | `varchar` |  |  | STO_UDFCHAR11 |
| `sto_udfchar12` | `string` | `varchar` |  |  | STO_UDFCHAR12 |
| `sto_udfchar13` | `string` | `varchar` |  |  | STO_UDFCHAR13 |
| `sto_udfchar14` | `string` | `varchar` |  |  | STO_UDFCHAR14 |
| `sto_udfchar15` | `string` | `varchar` |  |  | STO_UDFCHAR15 |
| `sto_udfchar16` | `string` | `varchar` |  |  | STO_UDFCHAR16 |
| `sto_udfchar17` | `string` | `varchar` |  |  | STO_UDFCHAR17 |
| `sto_udfchar18` | `string` | `varchar` |  |  | STO_UDFCHAR18 |
| `sto_udfchar19` | `string` | `varchar` |  |  | STO_UDFCHAR19 |
| `sto_udfchar20` | `string` | `varchar` |  |  | STO_UDFCHAR20 |
| `sto_udfchar21` | `string` | `varchar` |  |  | STO_UDFCHAR21 |
| `sto_udfchar22` | `string` | `varchar` |  |  | STO_UDFCHAR22 |
| `sto_udfchar23` | `string` | `varchar` |  |  | STO_UDFCHAR23 |
| `sto_udfchar24` | `string` | `varchar` |  |  | STO_UDFCHAR24 |
| `sto_udfchar25` | `string` | `varchar` |  |  | STO_UDFCHAR25 |
| `sto_udfchar26` | `string` | `varchar` |  |  | STO_UDFCHAR26 |
| `sto_udfchar27` | `string` | `varchar` |  |  | STO_UDFCHAR27 |
| `sto_udfchar28` | `string` | `varchar` |  |  | STO_UDFCHAR28 |
| `sto_udfchar29` | `string` | `varchar` |  |  | STO_UDFCHAR29 |
| `sto_udfchar30` | `string` | `varchar` |  |  | STO_UDFCHAR30 |
| `sto_udfnum01` | `float` | `float` |  |  | STO_UDFNUM01 |
| `sto_udfnum02` | `float` | `float` |  |  | STO_UDFNUM02 |
| `sto_udfnum03` | `float` | `float` |  |  | STO_UDFNUM03 |
| `sto_udfnum04` | `float` | `float` |  |  | STO_UDFNUM04 |
| `sto_udfnum05` | `float` | `float` |  |  | STO_UDFNUM05 |
| `sto_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | STO_UDFDATE01 |
| `sto_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | STO_UDFDATE02 |
| `sto_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | STO_UDFDATE03 |
| `sto_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | STO_UDFDATE04 |
| `sto_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | STO_UDFDATE05 |
| `sto_udfchkbox01` | `string` | `varchar` |  |  | STO_UDFCHKBOX01 |
| `sto_udfchkbox02` | `string` | `varchar` |  |  | STO_UDFCHKBOX02 |
| `sto_udfchkbox03` | `string` | `varchar` |  |  | STO_UDFCHKBOX03 |
| `sto_udfchkbox04` | `string` | `varchar` |  |  | STO_UDFCHKBOX04 |
| `sto_udfchkbox05` | `string` | `varchar` |  |  | STO_UDFCHKBOX05 |
| `sto_preventissuedefrtnbin` | `string` | `varchar` |  |  | STO_PREVENTISSUEDEFRTNBIN |
| `sto_defaultreturnbin` | `string` | `varchar` |  |  | STO_DEFAULTRETURNBIN |
| `sto_partcondition` | `string` | `varchar` |  |  | STO_PARTCONDITION |
| `sto_reorderconditions` | `string` | `varchar` |  |  | STO_REORDERCONDITIONS |
| `sto_corereturntemplate` | `string` | `varchar` |  |  | STO_CORERETURNTEMPLATE |
| `sto_warrantytemplate` | `string` | `varchar` |  |  | STO_WARRANTYTEMPLATE |
| `sto_helditemspotemplate` | `string` | `varchar` |  |  | STO_HELDITEMSPOTEMPLATE |
| `sto_helditemsnonpotemplate` | `string` | `varchar` |  |  | STO_HELDITEMSNONPOTEMPLATE |
| `sto_helditemsstktrstemplate` | `string` | `varchar` |  |  | STO_HELDITEMSSTKTRSTEMPLATE |
| `sto_helditemstrstemplate` | `string` | `varchar` |  |  | STO_HELDITEMSTRSTEMPLATE |
| `sto_helditemstostktrstemplate` | `string` | `varchar` |  |  | STO_HELDITEMSTOSTKTRSTEMPLATE |
| `sto_helditemsreceipttemplate` | `string` | `varchar` |  |  | STO_HELDITEMSRECEIPTTEMPLATE |
| `sto_custandardprice` | `float` | `float` |  |  | STO_CUSTANDARDPRICE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

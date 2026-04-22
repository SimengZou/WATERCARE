# r5translines

eam_R5TRANSLINES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5translines` |
| **Materialization** | `incremental` |
| **Primary keys** | `trl_trans`, `trl_line` |
| **Column count** | 146 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `trl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | TRL_LASTSAVED |
| `trl_trans` | `string` | `varchar` | `PK` |  | TRL_TRANS |
| `trl_type` | `string` | `varchar` |  |  | TRL_TYPE |
| `trl_rtype` | `string` | `varchar` |  |  | TRL_RTYPE |
| `trl_line` | `float` | `float` | `PK` |  | TRL_LINE |
| `trl_date` | `timestamp` | `timestamp_ntz` |  |  | TRL_DATE |
| `trl_costcode` | `string` | `varchar` |  |  | TRL_COSTCODE |
| `trl_project` | `string` | `varchar` |  |  | TRL_PROJECT |
| `trl_projbud` | `string` | `varchar` |  |  | TRL_PROJBUD |
| `trl_event` | `string` | `varchar` |  |  | TRL_EVENT |
| `trl_act` | `float` | `float` |  |  | TRL_ACT |
| `trl_part` | `string` | `varchar` |  |  | TRL_PART |
| `trl_ordline` | `float` | `float` |  |  | TRL_ORDLINE |
| `trl_reqline` | `float` | `float` |  |  | TRL_REQLINE |
| `trl_obtype` | `string` | `varchar` |  |  | TRL_OBTYPE |
| `trl_obrtype` | `string` | `varchar` |  |  | TRL_OBRTYPE |
| `trl_object` | `string` | `varchar` |  |  | TRL_OBJECT |
| `trl_lot` | `string` | `varchar` |  |  | TRL_LOT |
| `trl_bin` | `string` | `varchar` |  |  | TRL_BIN |
| `trl_store` | `string` | `varchar` |  |  | TRL_STORE |
| `trl_price` | `float` | `float` |  |  | TRL_PRICE |
| `trl_qty` | `float` | `float` |  |  | TRL_QTY |
| `trl_stockprice` | `float` | `float` |  |  | TRL_STOCKPRICE |
| `trl_order` | `string` | `varchar` |  |  | TRL_ORDER |
| `trl_req` | `string` | `varchar` |  |  | TRL_REQ |
| `trl_multiply` | `float` | `float` |  |  | TRL_MULTIPLY |
| `trl_io` | `float` | `float` |  |  | TRL_IO |
| `trl_acd` | `float` | `float` |  |  | TRL_ACD |
| `trl_gltransferflag` | `string` | `varchar` |  |  | TRL_GLTRANSFERFLAG |
| `trl_gltransfer` | `timestamp` | `timestamp_ntz` |  |  | TRL_GLTRANSFER |
| `trl_interface` | `timestamp` | `timestamp_ntz` |  |  | TRL_INTERFACE |
| `trl_sourcesystem` | `string` | `varchar` |  |  | TRL_SOURCESYSTEM |
| `trl_sourcecode` | `string` | `varchar` |  |  | TRL_SOURCECODE |
| `trl_consignment` | `string` | `varchar` |  |  | TRL_CONSIGNMENT |
| `trl_consignsupplier` | `string` | `varchar` |  |  | TRL_CONSIGNSUPPLIER |
| `trl_rejectreason` | `string` | `varchar` |  |  | TRL_REJECTREASON |
| `trl_transorgid` | `float` | `float` |  |  | TRL_TRANSORGID |
| `trl_transcode` | `string` | `varchar` |  |  | TRL_TRANSCODE |
| `trl_transgroup` | `float` | `float` |  |  | TRL_TRANSGROUP |
| `trl_warranty` | `string` | `varchar` |  |  | TRL_WARRANTY |
| `trl_object_org` | `string` | `varchar` |  |  | TRL_OBJECT_ORG |
| `trl_consignsupplier_org` | `string` | `varchar` |  |  | TRL_CONSIGNSUPPLIER_ORG |
| `trl_order_org` | `string` | `varchar` |  |  | TRL_ORDER_ORG |
| `trl_part_org` | `string` | `varchar` |  |  | TRL_PART_ORG |
| `trl_picklist` | `string` | `varchar` |  |  | TRL_PICKLIST |
| `trl_dckcode` | `string` | `varchar` |  |  | TRL_DCKCODE |
| `trl_dckline` | `float` | `float` |  |  | TRL_DCKLINE |
| `trl_allocate` | `string` | `varchar` |  |  | TRL_ALLOCATE |
| `trl_repairprice` | `float` | `float` |  |  | TRL_REPAIRPRICE |
| `trl_scrapqty` | `float` | `float` |  |  | TRL_SCRAPQTY |
| `trl_evtalias` | `string` | `varchar` |  |  | TRL_EVTALIAS |
| `trl_printqty` | `float` | `float` |  |  | TRL_PRINTQTY |
| `trl_priceupdate` | `string` | `varchar` |  |  | TRL_PRICEUPDATE |
| `trl_updatecount` | `float` | `float` |  |  | TRL_UPDATECOUNT |
| `trl_origqty` | `float` | `float` |  |  | TRL_ORIGQTY |
| `trl_routerec` | `string` | `varchar` |  |  | TRL_ROUTEREC |
| `trl_created` | `timestamp` | `timestamp_ntz` |  |  | TRL_CREATED |
| `trl_updated` | `timestamp` | `timestamp_ntz` |  |  | TRL_UPDATED |
| `trl_newmrc` | `string` | `varchar` |  |  | TRL_NEWMRC |
| `trl_newcostcode` | `string` | `varchar` |  |  | TRL_NEWCOSTCODE |
| `trl_newmanufact` | `string` | `varchar` |  |  | TRL_NEWMANUFACT |
| `trl_avgprice` | `float` | `float` |  |  | TRL_AVGPRICE |
| `trl_jecategory` | `string` | `varchar` |  |  | TRL_JECATEGORY |
| `trl_jesource` | `string` | `varchar` |  |  | TRL_JESOURCE |
| `trl_billsubline` | `string` | `varchar` |  |  | TRL_BILLSUBLINE |
| `trl_fleetchecked` | `string` | `varchar` |  |  | TRL_FLEETCHECKED |
| `trl_fleetmarkup` | `float` | `float` |  |  | TRL_FLEETMARKUP |
| `trl_splittrans` | `string` | `varchar` |  |  | TRL_SPLITTRANS |
| `trl_desc` | `string` | `varchar` |  |  | TRL_DESC |
| `trl_attachto` | `string` | `varchar` |  |  | TRL_ATTACHTO |
| `trl_attachto_org` | `string` | `varchar` |  |  | TRL_ATTACHTO_ORG |
| `trl_attach` | `string` | `varchar` |  |  | TRL_ATTACH |
| `trl_scrapobject` | `string` | `varchar` |  |  | TRL_SCRAPOBJECT |
| `trl_scrapobject_org` | `string` | `varchar` |  |  | TRL_SCRAPOBJECT_ORG |
| `trl_manufacturer` | `string` | `varchar` |  |  | TRL_MANUFACTURER |
| `trl_manufactpart` | `string` | `varchar` |  |  | TRL_MANUFACTPART |
| `trl_leak` | `string` | `varchar` |  |  | TRL_LEAK |
| `trl_udfchar01` | `string` | `varchar` |  |  | TRL_UDFCHAR01 |
| `trl_udfchar02` | `string` | `varchar` |  |  | TRL_UDFCHAR02 |
| `trl_udfchar03` | `string` | `varchar` |  |  | TRL_UDFCHAR03 |
| `trl_udfchar04` | `string` | `varchar` |  |  | TRL_UDFCHAR04 |
| `trl_udfchar05` | `string` | `varchar` |  |  | TRL_UDFCHAR05 |
| `trl_udfchar06` | `string` | `varchar` |  |  | TRL_UDFCHAR06 |
| `trl_udfchar07` | `string` | `varchar` |  |  | TRL_UDFCHAR07 |
| `trl_udfchar08` | `string` | `varchar` |  |  | TRL_UDFCHAR08 |
| `trl_udfchar09` | `string` | `varchar` |  |  | TRL_UDFCHAR09 |
| `trl_udfchar10` | `string` | `varchar` |  |  | TRL_UDFCHAR10 |
| `trl_udfchar11` | `string` | `varchar` |  |  | TRL_UDFCHAR11 |
| `trl_udfchar12` | `string` | `varchar` |  |  | TRL_UDFCHAR12 |
| `trl_udfchar14` | `string` | `varchar` |  |  | TRL_UDFCHAR14 |
| `trl_udfchar15` | `string` | `varchar` |  |  | TRL_UDFCHAR15 |
| `trl_udfchar16` | `string` | `varchar` |  |  | TRL_UDFCHAR16 |
| `trl_udfchar17` | `string` | `varchar` |  |  | TRL_UDFCHAR17 |
| `trl_udfchar18` | `string` | `varchar` |  |  | TRL_UDFCHAR18 |
| `trl_udfchar19` | `string` | `varchar` |  |  | TRL_UDFCHAR19 |
| `trl_udfchar20` | `string` | `varchar` |  |  | TRL_UDFCHAR20 |
| `trl_udfchar21` | `string` | `varchar` |  |  | TRL_UDFCHAR21 |
| `trl_udfchar22` | `string` | `varchar` |  |  | TRL_UDFCHAR22 |
| `trl_udfchar23` | `string` | `varchar` |  |  | TRL_UDFCHAR23 |
| `trl_udfchar24` | `string` | `varchar` |  |  | TRL_UDFCHAR24 |
| `trl_udfchar25` | `string` | `varchar` |  |  | TRL_UDFCHAR25 |
| `trl_udfchar26` | `string` | `varchar` |  |  | TRL_UDFCHAR26 |
| `trl_udfchar27` | `string` | `varchar` |  |  | TRL_UDFCHAR27 |
| `trl_udfchar28` | `string` | `varchar` |  |  | TRL_UDFCHAR28 |
| `trl_udfchar29` | `string` | `varchar` |  |  | TRL_UDFCHAR29 |
| `trl_udfchar30` | `string` | `varchar` |  |  | TRL_UDFCHAR30 |
| `trl_udfnum01` | `float` | `float` |  |  | TRL_UDFNUM01 |
| `trl_udfnum02` | `float` | `float` |  |  | TRL_UDFNUM02 |
| `trl_udfnum03` | `float` | `float` |  |  | TRL_UDFNUM03 |
| `trl_udfnum04` | `float` | `float` |  |  | TRL_UDFNUM04 |
| `trl_udfnum05` | `float` | `float` |  |  | TRL_UDFNUM05 |
| `trl_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | TRL_UDFDATE01 |
| `trl_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | TRL_UDFDATE02 |
| `trl_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | TRL_UDFDATE03 |
| `trl_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | TRL_UDFDATE04 |
| `trl_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | TRL_UDFDATE05 |
| `trl_udfchkbox01` | `string` | `varchar` |  |  | TRL_UDFCHKBOX01 |
| `trl_udfchkbox02` | `string` | `varchar` |  |  | TRL_UDFCHKBOX02 |
| `trl_udfchkbox03` | `string` | `varchar` |  |  | TRL_UDFCHKBOX03 |
| `trl_udfchkbox04` | `string` | `varchar` |  |  | TRL_UDFCHKBOX04 |
| `trl_udfchkbox05` | `string` | `varchar` |  |  | TRL_UDFCHKBOX05 |
| `trl_invoice` | `string` | `varchar` |  |  | TRL_INVOICE |
| `trl_invoice_org` | `string` | `varchar` |  |  | TRL_INVOICE_ORG |
| `trl_invoice_revision` | `float` | `float` |  |  | TRL_INVOICE_REVISION |
| `trl_invoiceline` | `float` | `float` |  |  | TRL_INVOICELINE |
| `trl_issuedpart` | `string` | `varchar` |  |  | TRL_ISSUEDPART |
| `trl_corereturnprintqty` | `float` | `float` |  |  | TRL_CORERETURNPRINTQTY |
| `trl_poextracharges` | `float` | `float` |  |  | TRL_POEXTRACHARGES |
| `trl_potaxamount` | `float` | `float` |  |  | TRL_POTAXAMOUNT |
| `trl_partlocation` | `string` | `varchar` |  |  | TRL_PARTLOCATION |
| `trl_supplier` | `string` | `varchar` |  |  | TRL_SUPPLIER |
| `trl_supplier_org` | `string` | `varchar` |  |  | TRL_SUPPLIER_ORG |
| `trl_package` | `string` | `varchar` |  |  | TRL_PACKAGE |
| `trl_helditem` | `string` | `varchar` |  |  | TRL_HELDITEM |
| `trl_calothercost` | `float` | `float` |  |  | TRL_CALOTHERCOST |
| `trl_taxjurisdiction` | `string` | `varchar` |  |  | TRL_TAXJURISDICTION |
| `trl_stocktake` | `string` | `varchar` |  |  | TRL_STOCKTAKE |
| `trl_stocktakeaction` | `string` | `varchar` |  |  | TRL_STOCKTAKEACTION |
| `trl_receivereq` | `string` | `varchar` |  |  | TRL_RECEIVEREQ |
| `trl_udfchar13` | `string` | `varchar` |  |  | TRL_UDFCHAR13 |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

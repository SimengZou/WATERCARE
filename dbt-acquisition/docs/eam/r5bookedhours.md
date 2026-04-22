# r5bookedhours

eam_R5BOOKEDHOURS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5bookedhours` |
| **Materialization** | `incremental` |
| **Primary keys** | `boo_code` |
| **Column count** | 106 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `boo_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | BOO_LASTSAVED |
| `boo_gltransferflag` | `string` | `varchar` |  |  | BOO_GLTRANSFERFLAG |
| `boo_gltransfer` | `timestamp` | `timestamp_ntz` |  |  | BOO_GLTRANSFER |
| `boo_transorgid` | `float` | `float` |  |  | BOO_TRANSORGID |
| `boo_transcode` | `string` | `varchar` |  |  | BOO_TRANSCODE |
| `boo_transgroup` | `float` | `float` |  |  | BOO_TRANSGROUP |
| `boo_correction` | `string` | `varchar` |  |  | BOO_CORRECTION |
| `boo_order` | `string` | `varchar` |  |  | BOO_ORDER |
| `boo_ordline` | `float` | `float` |  |  | BOO_ORDLINE |
| `boo_order_org` | `string` | `varchar` |  |  | BOO_ORDER_ORG |
| `boo_evtalias` | `string` | `varchar` |  |  | BOO_EVTALIAS |
| `boo_updatecount` | `float` | `float` |  |  | BOO_UPDATECOUNT |
| `boo_orighours` | `float` | `float` |  |  | BOO_ORIGHOURS |
| `boo_routertype` | `string` | `varchar` |  |  | BOO_ROUTERTYPE |
| `boo_routerec` | `string` | `varchar` |  |  | BOO_ROUTEREC |
| `boo_billsubline` | `string` | `varchar` |  |  | BOO_BILLSUBLINE |
| `boo_fleetchecked` | `string` | `varchar` |  |  | BOO_FLEETCHECKED |
| `boo_jecategory` | `string` | `varchar` |  |  | BOO_JECATEGORY |
| `boo_jesource` | `string` | `varchar` |  |  | BOO_JESOURCE |
| `boo_fleetmarkup` | `float` | `float` |  |  | BOO_FLEETMARKUP |
| `boo_splithours` | `string` | `varchar` |  |  | BOO_SPLITHOURS |
| `boo_desc` | `string` | `varchar` |  |  | BOO_DESC |
| `boo_code` | `string` | `varchar` | `PK` | `unique` | BOO_CODE |
| `boo_routeparent` | `string` | `varchar` |  |  | BOO_ROUTEPARENT |
| `boo_misc` | `string` | `varchar` |  |  | BOO_MISC |
| `boo_origtaxamount` | `float` | `float` |  |  | BOO_ORIGTAXAMOUNT |
| `boo_taxamount` | `float` | `float` |  |  | BOO_TAXAMOUNT |
| `boo_udfchar01` | `string` | `varchar` |  |  | BOO_UDFCHAR01 |
| `boo_udfchar02` | `string` | `varchar` |  |  | BOO_UDFCHAR02 |
| `boo_udfchar03` | `string` | `varchar` |  |  | BOO_UDFCHAR03 |
| `boo_udfchar04` | `string` | `varchar` |  |  | BOO_UDFCHAR04 |
| `boo_udfchar05` | `string` | `varchar` |  |  | BOO_UDFCHAR05 |
| `boo_udfchar06` | `string` | `varchar` |  |  | BOO_UDFCHAR06 |
| `boo_udfchar07` | `string` | `varchar` |  |  | BOO_UDFCHAR07 |
| `boo_udfchar08` | `string` | `varchar` |  |  | BOO_UDFCHAR08 |
| `boo_udfchar09` | `string` | `varchar` |  |  | BOO_UDFCHAR09 |
| `boo_udfchar10` | `string` | `varchar` |  |  | BOO_UDFCHAR10 |
| `boo_udfchar11` | `string` | `varchar` |  |  | BOO_UDFCHAR11 |
| `boo_udfchar12` | `string` | `varchar` |  |  | BOO_UDFCHAR12 |
| `boo_udfchar13` | `string` | `varchar` |  |  | BOO_UDFCHAR13 |
| `boo_udfchar14` | `string` | `varchar` |  |  | BOO_UDFCHAR14 |
| `boo_udfchar15` | `string` | `varchar` |  |  | BOO_UDFCHAR15 |
| `boo_udfchar16` | `string` | `varchar` |  |  | BOO_UDFCHAR16 |
| `boo_udfchar17` | `string` | `varchar` |  |  | BOO_UDFCHAR17 |
| `boo_udfchar18` | `string` | `varchar` |  |  | BOO_UDFCHAR18 |
| `boo_udfchar19` | `string` | `varchar` |  |  | BOO_UDFCHAR19 |
| `boo_udfchar20` | `string` | `varchar` |  |  | BOO_UDFCHAR20 |
| `boo_udfchar21` | `string` | `varchar` |  |  | BOO_UDFCHAR21 |
| `boo_udfchar22` | `string` | `varchar` |  |  | BOO_UDFCHAR22 |
| `boo_udfchar23` | `string` | `varchar` |  |  | BOO_UDFCHAR23 |
| `boo_udfchar24` | `string` | `varchar` |  |  | BOO_UDFCHAR24 |
| `boo_udfchar25` | `string` | `varchar` |  |  | BOO_UDFCHAR25 |
| `boo_udfchar26` | `string` | `varchar` |  |  | BOO_UDFCHAR26 |
| `boo_udfchar27` | `string` | `varchar` |  |  | BOO_UDFCHAR27 |
| `boo_udfchar28` | `string` | `varchar` |  |  | BOO_UDFCHAR28 |
| `boo_udfchar29` | `string` | `varchar` |  |  | BOO_UDFCHAR29 |
| `boo_udfchar30` | `string` | `varchar` |  |  | BOO_UDFCHAR30 |
| `boo_udfnum01` | `float` | `float` |  |  | BOO_UDFNUM01 |
| `boo_udfnum02` | `float` | `float` |  |  | BOO_UDFNUM02 |
| `boo_udfnum03` | `float` | `float` |  |  | BOO_UDFNUM03 |
| `boo_udfnum04` | `float` | `float` |  |  | BOO_UDFNUM04 |
| `boo_udfnum05` | `float` | `float` |  |  | BOO_UDFNUM05 |
| `boo_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | BOO_UDFDATE01 |
| `boo_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | BOO_UDFDATE02 |
| `boo_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | BOO_UDFDATE03 |
| `boo_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | BOO_UDFDATE04 |
| `boo_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | BOO_UDFDATE05 |
| `boo_udfchkbox01` | `string` | `varchar` |  |  | BOO_UDFCHKBOX01 |
| `boo_udfchkbox02` | `string` | `varchar` |  |  | BOO_UDFCHKBOX02 |
| `boo_udfchkbox03` | `string` | `varchar` |  |  | BOO_UDFCHKBOX03 |
| `boo_udfchkbox04` | `string` | `varchar` |  |  | BOO_UDFCHKBOX04 |
| `boo_udfchkbox05` | `string` | `varchar` |  |  | BOO_UDFCHKBOX05 |
| `boo_invoice` | `string` | `varchar` |  |  | BOO_INVOICE |
| `boo_invoice_org` | `string` | `varchar` |  |  | BOO_INVOICE_ORG |
| `boo_invoice_revision` | `float` | `float` |  |  | BOO_INVOICE_REVISION |
| `boo_invoiceline` | `float` | `float` |  |  | BOO_INVOICELINE |
| `boo_correctiondate` | `timestamp` | `timestamp_ntz` |  |  | BOO_CORRECTIONDATE |
| `boo_unmatchedinvoice` | `string` | `varchar` |  |  | BOO_UNMATCHEDINVOICE |
| `boo_unmatchedinvoice_org` | `string` | `varchar` |  |  | BOO_UNMATCHEDINVOICE_ORG |
| `boo_unmatchedinvoiceline` | `float` | `float` |  |  | BOO_UNMATCHEDINVOICELINE |
| `boo_currentcrew` | `string` | `varchar` |  |  | BOO_CURRENTCREW |
| `boo_currentcrew_org` | `string` | `varchar` |  |  | BOO_CURRENTCREW_ORG |
| `boo_crew` | `string` | `varchar` |  |  | BOO_CREW |
| `boo_crew_org` | `string` | `varchar` |  |  | BOO_CREW_ORG |
| `boo_correction_ref` | `string` | `varchar` |  |  | BOO_CORRECTION_REF |
| `boo_event` | `string` | `varchar` |  |  | BOO_EVENT |
| `boo_act` | `float` | `float` |  |  | BOO_ACT |
| `boo_date` | `timestamp` | `timestamp_ntz` |  |  | BOO_DATE |
| `boo_trade` | `string` | `varchar` |  |  | BOO_TRADE |
| `boo_mrc` | `string` | `varchar` |  |  | BOO_MRC |
| `boo_person` | `string` | `varchar` |  |  | BOO_PERSON |
| `boo_octype` | `string` | `varchar` |  |  | BOO_OCTYPE |
| `boo_ocrtype` | `string` | `varchar` |  |  | BOO_OCRTYPE |
| `boo_on` | `float` | `float` |  |  | BOO_ON |
| `boo_off` | `float` | `float` |  |  | BOO_OFF |
| `boo_hours` | `float` | `float` |  |  | BOO_HOURS |
| `boo_rate` | `float` | `float` |  |  | BOO_RATE |
| `boo_cost` | `float` | `float` |  |  | BOO_COST |
| `boo_entered` | `timestamp` | `timestamp_ntz` |  |  | BOO_ENTERED |
| `boo_acd` | `float` | `float` |  |  | BOO_ACD |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

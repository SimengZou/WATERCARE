# r5orlrevisions

eam_R5ORLREVISIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5orlrevisions` |
| **Materialization** | `incremental` |
| **Primary keys** | `olr_order`, `olr_order_org`, `olr_revision`, `olr_ordline` |
| **Column count** | 128 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `olr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | OLR_LASTSAVED |
| `olr_order` | `string` | `varchar` | `PK` |  | OLR_ORDER |
| `olr_ordline` | `float` | `float` | `PK` |  | OLR_ORDLINE |
| `olr_revision` | `float` | `float` | `PK` |  | OLR_REVISION |
| `olr_revised` | `timestamp` | `timestamp_ntz` |  |  | OLR_REVISED |
| `olr_revisedline` | `string` | `varchar` |  |  | OLR_REVISEDLINE |
| `olr_revisedby` | `string` | `varchar` |  |  | OLR_REVISEDBY |
| `olr_req` | `string` | `varchar` |  |  | OLR_REQ |
| `olr_reqline` | `float` | `float` |  |  | OLR_REQLINE |
| `olr_supplier` | `string` | `varchar` |  |  | OLR_SUPPLIER |
| `olr_part` | `string` | `varchar` |  |  | OLR_PART |
| `olr_due` | `timestamp` | `timestamp_ntz` |  |  | OLR_DUE |
| `olr_price` | `float` | `float` |  |  | OLR_PRICE |
| `olr_curr` | `string` | `varchar` |  |  | OLR_CURR |
| `olr_exch` | `float` | `float` |  |  | OLR_EXCH |
| `olr_ordqty` | `float` | `float` |  |  | OLR_ORDQTY |
| `olr_recvqty` | `float` | `float` |  |  | OLR_RECVQTY |
| `olr_recvvalue` | `float` | `float` |  |  | OLR_RECVVALUE |
| `olr_invqty` | `float` | `float` |  |  | OLR_INVQTY |
| `olr_costcode` | `string` | `varchar` |  |  | OLR_COSTCODE |
| `olr_project` | `string` | `varchar` |  |  | OLR_PROJECT |
| `olr_projbud` | `string` | `varchar` |  |  | OLR_PROJBUD |
| `olr_contract` | `string` | `varchar` |  |  | OLR_CONTRACT |
| `olr_discperc` | `float` | `float` |  |  | OLR_DISCPERC |
| `olr_event` | `string` | `varchar` |  |  | OLR_EVENT |
| `olr_act` | `float` | `float` |  |  | OLR_ACT |
| `olr_store` | `string` | `varchar` |  |  | OLR_STORE |
| `olr_puruom` | `string` | `varchar` |  |  | OLR_PURUOM |
| `olr_invvalue` | `float` | `float` |  |  | OLR_INVVALUE |
| `olr_rtype` | `string` | `varchar` |  |  | OLR_RTYPE |
| `olr_type` | `string` | `varchar` |  |  | OLR_TYPE |
| `olr_active` | `string` | `varchar` |  |  | OLR_ACTIVE |
| `olr_multiply` | `float` | `float` |  |  | OLR_MULTIPLY |
| `olr_tax` | `string` | `varchar` |  |  | OLR_TAX |
| `olr_rstatus` | `string` | `varchar` |  |  | OLR_RSTATUS |
| `olr_status` | `string` | `varchar` |  |  | OLR_STATUS |
| `olr_blanketorder` | `string` | `varchar` |  |  | OLR_BLANKETORDER |
| `olr_blanketline` | `float` | `float` |  |  | OLR_BLANKETLINE |
| `olr_exchtodual` | `float` | `float` |  |  | OLR_EXCHTODUAL |
| `olr_exchfromdual` | `float` | `float` |  |  | OLR_EXCHFROMDUAL |
| `olr_ref` | `string` | `varchar` |  |  | OLR_REF |
| `olr_task` | `string` | `varchar` |  |  | OLR_TASK |
| `olr_taskrev` | `float` | `float` |  |  | OLR_TASKREV |
| `olr_taskqty` | `float` | `float` |  |  | OLR_TASKQTY |
| `olr_trade` | `string` | `varchar` |  |  | OLR_TRADE |
| `olr_recvtaskqty` | `float` | `float` |  |  | OLR_RECVTASKQTY |
| `olr_deladdress` | `string` | `varchar` |  |  | OLR_DELADDRESS |
| `olr_supplier_org` | `string` | `varchar` |  |  | OLR_SUPPLIER_ORG |
| `olr_order_org` | `string` | `varchar` | `PK` |  | OLR_ORDER_ORG |
| `olr_part_org` | `string` | `varchar` |  |  | OLR_PART_ORG |
| `olr_inspect` | `string` | `varchar` |  |  | OLR_INSPECT |
| `olr_exchfix` | `string` | `varchar` |  |  | OLR_EXCHFIX |
| `olr_tax2` | `string` | `varchar` |  |  | OLR_TAX2 |
| `olr_tottaxamount` | `float` | `float` |  |  | OLR_TOTTAXAMOUNT |
| `olr_totextra` | `float` | `float` |  |  | OLR_TOTEXTRA |
| `olr_scrapqty` | `float` | `float` |  |  | OLR_SCRAPQTY |
| `olr_updatecount` | `float` | `float` |  |  | OLR_UPDATECOUNT |
| `olr_dckrecvqty` | `float` | `float` |  |  | OLR_DCKRECVQTY |
| `olr_relatedwo` | `string` | `varchar` |  |  | OLR_RELATEDWO |
| `olr_quotid` | `string` | `varchar` |  |  | OLR_QUOTID |
| `olr_quotline` | `float` | `float` |  |  | OLR_QUOTLINE |
| `olr_manufacturer` | `string` | `varchar` |  |  | OLR_MANUFACTURER |
| `olr_manufactpart` | `string` | `varchar` |  |  | OLR_MANUFACTPART |
| `olr_udfchar01` | `string` | `varchar` |  |  | OLR_UDFCHAR01 |
| `olr_udfchar02` | `string` | `varchar` |  |  | OLR_UDFCHAR02 |
| `olr_udfchar03` | `string` | `varchar` |  |  | OLR_UDFCHAR03 |
| `olr_udfchar04` | `string` | `varchar` |  |  | OLR_UDFCHAR04 |
| `olr_udfchar05` | `string` | `varchar` |  |  | OLR_UDFCHAR05 |
| `olr_udfchar06` | `string` | `varchar` |  |  | OLR_UDFCHAR06 |
| `olr_udfchar07` | `string` | `varchar` |  |  | OLR_UDFCHAR07 |
| `olr_udfchar08` | `string` | `varchar` |  |  | OLR_UDFCHAR08 |
| `olr_udfchar09` | `string` | `varchar` |  |  | OLR_UDFCHAR09 |
| `olr_udfchar10` | `string` | `varchar` |  |  | OLR_UDFCHAR10 |
| `olr_udfchar11` | `string` | `varchar` |  |  | OLR_UDFCHAR11 |
| `olr_udfchar12` | `string` | `varchar` |  |  | OLR_UDFCHAR12 |
| `olr_udfchar13` | `string` | `varchar` |  |  | OLR_UDFCHAR13 |
| `olr_udfchar14` | `string` | `varchar` |  |  | OLR_UDFCHAR14 |
| `olr_udfchar15` | `string` | `varchar` |  |  | OLR_UDFCHAR15 |
| `olr_udfchar16` | `string` | `varchar` |  |  | OLR_UDFCHAR16 |
| `olr_udfchar17` | `string` | `varchar` |  |  | OLR_UDFCHAR17 |
| `olr_udfchar18` | `string` | `varchar` |  |  | OLR_UDFCHAR18 |
| `olr_udfchar19` | `string` | `varchar` |  |  | OLR_UDFCHAR19 |
| `olr_udfchar20` | `string` | `varchar` |  |  | OLR_UDFCHAR20 |
| `olr_udfchar21` | `string` | `varchar` |  |  | OLR_UDFCHAR21 |
| `olr_udfchar22` | `string` | `varchar` |  |  | OLR_UDFCHAR22 |
| `olr_udfchar23` | `string` | `varchar` |  |  | OLR_UDFCHAR23 |
| `olr_udfchar24` | `string` | `varchar` |  |  | OLR_UDFCHAR24 |
| `olr_udfchar25` | `string` | `varchar` |  |  | OLR_UDFCHAR25 |
| `olr_udfchar26` | `string` | `varchar` |  |  | OLR_UDFCHAR26 |
| `olr_udfchar27` | `string` | `varchar` |  |  | OLR_UDFCHAR27 |
| `olr_udfchar28` | `string` | `varchar` |  |  | OLR_UDFCHAR28 |
| `olr_udfchar29` | `string` | `varchar` |  |  | OLR_UDFCHAR29 |
| `olr_udfchar30` | `string` | `varchar` |  |  | OLR_UDFCHAR30 |
| `olr_udfnum01` | `float` | `float` |  |  | OLR_UDFNUM01 |
| `olr_udfnum02` | `float` | `float` |  |  | OLR_UDFNUM02 |
| `olr_udfnum03` | `float` | `float` |  |  | OLR_UDFNUM03 |
| `olr_udfnum04` | `float` | `float` |  |  | OLR_UDFNUM04 |
| `olr_udfnum05` | `float` | `float` |  |  | OLR_UDFNUM05 |
| `olr_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | OLR_UDFDATE01 |
| `olr_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | OLR_UDFDATE02 |
| `olr_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | OLR_UDFDATE03 |
| `olr_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | OLR_UDFDATE04 |
| `olr_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | OLR_UDFDATE05 |
| `olr_udfchkbox01` | `string` | `varchar` |  |  | OLR_UDFCHKBOX01 |
| `olr_udfchkbox02` | `string` | `varchar` |  |  | OLR_UDFCHKBOX02 |
| `olr_udfchkbox03` | `string` | `varchar` |  |  | OLR_UDFCHKBOX03 |
| `olr_udfchkbox04` | `string` | `varchar` |  |  | OLR_UDFCHKBOX04 |
| `olr_udfchkbox05` | `string` | `varchar` |  |  | OLR_UDFCHKBOX05 |
| `olr_packagetrackingnumber` | `string` | `varchar` |  |  | OLR_PACKAGETRACKINGNUMBER |
| `olr_allowprepayment` | `string` | `varchar` |  |  | OLR_ALLOWPREPAYMENT |
| `olr_paymentnumber` | `float` | `float` |  |  | OLR_PAYMENTNUMBER |
| `olr_sourcesystem` | `string` | `varchar` |  |  | OLR_SOURCESYSTEM |
| `olr_sourcecode` | `string` | `varchar` |  |  | OLR_SOURCECODE |
| `olr_acd` | `float` | `float` |  |  | OLR_ACD |
| `olr_interface` | `timestamp` | `timestamp_ntz` |  |  | OLR_INTERFACE |
| `olr_iptransmitted` | `string` | `varchar` |  |  | OLR_IPTRANSMITTED |
| `olr_ipcurrprice` | `string` | `varchar` |  |  | OLR_IPCURRPRICE |
| `olr_attentionto` | `string` | `varchar` |  |  | OLR_ATTENTIONTO |
| `olr_iperror` | `float` | `float` |  |  | OLR_IPERROR |
| `olr_iperrormessage` | `string` | `varchar` |  |  | OLR_IPERRORMESSAGE |
| `olr_note` | `string` | `varchar` |  |  | OLR_NOTE |
| `olr_longdescription` | `string` | `varchar` |  |  | OLR_LONGDESCRIPTION |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

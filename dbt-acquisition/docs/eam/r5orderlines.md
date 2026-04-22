# r5orderlines

eam_R5ORDERLINES

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5orderlines` |
| **Materialization** | `incremental` |
| **Primary keys** | `orl_order`, `orl_order_org`, `orl_ordline` |
| **Column count** | 134 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `orl_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ORL_LASTSAVED |
| `orl_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | ORL_UDFDATE01 |
| `orl_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | ORL_UDFDATE02 |
| `orl_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | ORL_UDFDATE03 |
| `orl_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | ORL_UDFDATE04 |
| `orl_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | ORL_UDFDATE05 |
| `orl_udfchkbox01` | `string` | `varchar` |  |  | ORL_UDFCHKBOX01 |
| `orl_udfchkbox02` | `string` | `varchar` |  |  | ORL_UDFCHKBOX02 |
| `orl_udfchkbox03` | `string` | `varchar` |  |  | ORL_UDFCHKBOX03 |
| `orl_udfchkbox04` | `string` | `varchar` |  |  | ORL_UDFCHKBOX04 |
| `orl_udfchkbox05` | `string` | `varchar` |  |  | ORL_UDFCHKBOX05 |
| `orl_packagetrackingnumber` | `string` | `varchar` |  |  | ORL_PACKAGETRACKINGNUMBER |
| `orl_allowprepayment` | `string` | `varchar` |  |  | ORL_ALLOWPREPAYMENT |
| `orl_paymentnumber` | `float` | `float` |  |  | ORL_PAYMENTNUMBER |
| `orl_note` | `string` | `varchar` |  |  | ORL_NOTE |
| `orl_longdescription` | `string` | `varchar` |  |  | ORL_LONGDESCRIPTION |
| `orl_unspsccode` | `string` | `varchar` |  |  | ORL_UNSPSCCODE |
| `orl_req` | `string` | `varchar` |  |  | ORL_REQ |
| `orl_reqline` | `float` | `float` |  |  | ORL_REQLINE |
| `orl_supplier` | `string` | `varchar` |  |  | ORL_SUPPLIER |
| `orl_part` | `string` | `varchar` |  |  | ORL_PART |
| `orl_due` | `timestamp` | `timestamp_ntz` |  |  | ORL_DUE |
| `orl_price` | `float` | `float` |  |  | ORL_PRICE |
| `orl_curr` | `string` | `varchar` |  |  | ORL_CURR |
| `orl_exch` | `float` | `float` |  |  | ORL_EXCH |
| `orl_ordqty` | `float` | `float` |  |  | ORL_ORDQTY |
| `orl_recvqty` | `float` | `float` |  |  | ORL_RECVQTY |
| `orl_recvvalue` | `float` | `float` |  |  | ORL_RECVVALUE |
| `orl_invqty` | `float` | `float` |  |  | ORL_INVQTY |
| `orl_contract` | `string` | `varchar` |  |  | ORL_CONTRACT |
| `orl_costcode` | `string` | `varchar` |  |  | ORL_COSTCODE |
| `orl_project` | `string` | `varchar` |  |  | ORL_PROJECT |
| `orl_projbud` | `string` | `varchar` |  |  | ORL_PROJBUD |
| `orl_discperc` | `float` | `float` |  |  | ORL_DISCPERC |
| `orl_event` | `string` | `varchar` |  |  | ORL_EVENT |
| `orl_act` | `float` | `float` |  |  | ORL_ACT |
| `orl_store` | `string` | `varchar` |  |  | ORL_STORE |
| `orl_puruom` | `string` | `varchar` |  |  | ORL_PURUOM |
| `orl_invvalue` | `float` | `float` |  |  | ORL_INVVALUE |
| `orl_rtype` | `string` | `varchar` |  |  | ORL_RTYPE |
| `orl_type` | `string` | `varchar` |  |  | ORL_TYPE |
| `orl_active` | `string` | `varchar` |  |  | ORL_ACTIVE |
| `orl_multiply` | `float` | `float` |  |  | ORL_MULTIPLY |
| `orl_tax` | `string` | `varchar` |  |  | ORL_TAX |
| `orl_rstatus` | `string` | `varchar` |  |  | ORL_RSTATUS |
| `orl_status` | `string` | `varchar` |  |  | ORL_STATUS |
| `orl_revision` | `float` | `float` |  |  | ORL_REVISION |
| `orl_revised` | `timestamp` | `timestamp_ntz` |  |  | ORL_REVISED |
| `orl_blanketorder` | `string` | `varchar` |  |  | ORL_BLANKETORDER |
| `orl_blanketline` | `float` | `float` |  |  | ORL_BLANKETLINE |
| `orl_sourcesystem` | `string` | `varchar` |  |  | ORL_SOURCESYSTEM |
| `orl_sourcecode` | `string` | `varchar` |  |  | ORL_SOURCECODE |
| `orl_acd` | `float` | `float` |  |  | ORL_ACD |
| `orl_interface` | `timestamp` | `timestamp_ntz` |  |  | ORL_INTERFACE |
| `orl_exchtodual` | `float` | `float` |  |  | ORL_EXCHTODUAL |
| `orl_exchfromdual` | `float` | `float` |  |  | ORL_EXCHFROMDUAL |
| `orl_ref` | `string` | `varchar` |  |  | ORL_REF |
| `orl_task` | `string` | `varchar` |  |  | ORL_TASK |
| `orl_taskrev` | `float` | `float` |  |  | ORL_TASKREV |
| `orl_taskqty` | `float` | `float` |  |  | ORL_TASKQTY |
| `orl_trade` | `string` | `varchar` |  |  | ORL_TRADE |
| `orl_recvtaskqty` | `float` | `float` |  |  | ORL_RECVTASKQTY |
| `orl_deladdress` | `string` | `varchar` |  |  | ORL_DELADDRESS |
| `orl_supplier_org` | `string` | `varchar` |  |  | ORL_SUPPLIER_ORG |
| `orl_order_org` | `string` | `varchar` | `PK` |  | ORL_ORDER_ORG |
| `orl_part_org` | `string` | `varchar` |  |  | ORL_PART_ORG |
| `orl_inspect` | `string` | `varchar` |  |  | ORL_INSPECT |
| `orl_dckrecvqty` | `float` | `float` |  |  | ORL_DCKRECVQTY |
| `orl_exchfix` | `string` | `varchar` |  |  | ORL_EXCHFIX |
| `orl_tax2` | `string` | `varchar` |  |  | ORL_TAX2 |
| `orl_tottaxamount` | `float` | `float` |  |  | ORL_TOTTAXAMOUNT |
| `orl_totextra` | `float` | `float` |  |  | ORL_TOTEXTRA |
| `orl_scrapqty` | `float` | `float` |  |  | ORL_SCRAPQTY |
| `orl_iptransmitted` | `string` | `varchar` |  |  | ORL_IPTRANSMITTED |
| `orl_ipcurrprice` | `string` | `varchar` |  |  | ORL_IPCURRPRICE |
| `orl_updatecount` | `float` | `float` |  |  | ORL_UPDATECOUNT |
| `orl_invcalctaxamount` | `float` | `float` |  |  | ORL_INVCALCTAXAMOUNT |
| `orl_jecategory` | `string` | `varchar` |  |  | ORL_JECATEGORY |
| `orl_jesource` | `string` | `varchar` |  |  | ORL_JESOURCE |
| `orl_gltransferflag` | `string` | `varchar` |  |  | ORL_GLTRANSFERFLAG |
| `orl_gltransfer` | `timestamp` | `timestamp_ntz` |  |  | ORL_GLTRANSFER |
| `orl_iplastupdate` | `timestamp` | `timestamp_ntz` |  |  | ORL_IPLASTUPDATE |
| `orl_ipupdatestatus` | `string` | `varchar` |  |  | ORL_IPUPDATESTATUS |
| `orl_relatedwo` | `string` | `varchar` |  |  | ORL_RELATEDWO |
| `orl_attentionto` | `string` | `varchar` |  |  | ORL_ATTENTIONTO |
| `orl_iperror` | `float` | `float` |  |  | ORL_IPERROR |
| `orl_iperrormessage` | `string` | `varchar` |  |  | ORL_IPERRORMESSAGE |
| `orl_quotid` | `string` | `varchar` |  |  | ORL_QUOTID |
| `orl_quotline` | `float` | `float` |  |  | ORL_QUOTLINE |
| `orl_manufacturer` | `string` | `varchar` |  |  | ORL_MANUFACTURER |
| `orl_manufactpart` | `string` | `varchar` |  |  | ORL_MANUFACTPART |
| `orl_udfchar01` | `string` | `varchar` |  |  | ORL_UDFCHAR01 |
| `orl_udfchar02` | `string` | `varchar` |  |  | ORL_UDFCHAR02 |
| `orl_udfchar03` | `string` | `varchar` |  |  | ORL_UDFCHAR03 |
| `orl_udfchar04` | `string` | `varchar` |  |  | ORL_UDFCHAR04 |
| `orl_udfchar05` | `string` | `varchar` |  |  | ORL_UDFCHAR05 |
| `orl_udfchar06` | `string` | `varchar` |  |  | ORL_UDFCHAR06 |
| `orl_udfchar07` | `string` | `varchar` |  |  | ORL_UDFCHAR07 |
| `orl_udfchar08` | `string` | `varchar` |  |  | ORL_UDFCHAR08 |
| `orl_udfchar09` | `string` | `varchar` |  |  | ORL_UDFCHAR09 |
| `orl_udfchar10` | `string` | `varchar` |  |  | ORL_UDFCHAR10 |
| `orl_udfchar11` | `string` | `varchar` |  |  | ORL_UDFCHAR11 |
| `orl_udfchar12` | `string` | `varchar` |  |  | ORL_UDFCHAR12 |
| `orl_udfchar13` | `string` | `varchar` |  |  | ORL_UDFCHAR13 |
| `orl_udfchar14` | `string` | `varchar` |  |  | ORL_UDFCHAR14 |
| `orl_udfchar15` | `string` | `varchar` |  |  | ORL_UDFCHAR15 |
| `orl_udfchar16` | `string` | `varchar` |  |  | ORL_UDFCHAR16 |
| `orl_udfchar17` | `string` | `varchar` |  |  | ORL_UDFCHAR17 |
| `orl_udfchar18` | `string` | `varchar` |  |  | ORL_UDFCHAR18 |
| `orl_udfchar19` | `string` | `varchar` |  |  | ORL_UDFCHAR19 |
| `orl_udfchar20` | `string` | `varchar` |  |  | ORL_UDFCHAR20 |
| `orl_udfchar21` | `string` | `varchar` |  |  | ORL_UDFCHAR21 |
| `orl_udfchar22` | `string` | `varchar` |  |  | ORL_UDFCHAR22 |
| `orl_udfchar23` | `string` | `varchar` |  |  | ORL_UDFCHAR23 |
| `orl_udfchar24` | `string` | `varchar` |  |  | ORL_UDFCHAR24 |
| `orl_udfchar25` | `string` | `varchar` |  |  | ORL_UDFCHAR25 |
| `orl_udfchar26` | `string` | `varchar` |  |  | ORL_UDFCHAR26 |
| `orl_udfchar27` | `string` | `varchar` |  |  | ORL_UDFCHAR27 |
| `orl_udfchar28` | `string` | `varchar` |  |  | ORL_UDFCHAR28 |
| `orl_udfchar29` | `string` | `varchar` |  |  | ORL_UDFCHAR29 |
| `orl_udfchar30` | `string` | `varchar` |  |  | ORL_UDFCHAR30 |
| `orl_udfnum01` | `float` | `float` |  |  | ORL_UDFNUM01 |
| `orl_udfnum02` | `float` | `float` |  |  | ORL_UDFNUM02 |
| `orl_udfnum03` | `float` | `float` |  |  | ORL_UDFNUM03 |
| `orl_udfnum04` | `float` | `float` |  |  | ORL_UDFNUM04 |
| `orl_udfnum05` | `float` | `float` |  |  | ORL_UDFNUM05 |
| `orl_order` | `string` | `varchar` | `PK` |  | ORL_ORDER |
| `orl_ordline` | `float` | `float` | `PK` |  | ORL_ORDLINE |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

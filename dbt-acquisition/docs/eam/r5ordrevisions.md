# r5ordrevisions

eam_R5ORDREVISIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5ordrevisions` |
| **Materialization** | `incremental` |
| **Primary keys** | `orr_order`, `orr_order_org`, `orr_revision` |
| **Column count** | 109 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `orr_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ORR_LASTSAVED |
| `orr_order` | `string` | `varchar` | `PK` |  | ORR_ORDER |
| `orr_revision` | `float` | `float` | `PK` |  | ORR_REVISION |
| `orr_revised` | `timestamp` | `timestamp_ntz` |  |  | ORR_REVISED |
| `orr_revisedorder` | `string` | `varchar` |  |  | ORR_REVISEDORDER |
| `orr_revisedby` | `string` | `varchar` |  |  | ORR_REVISEDBY |
| `orr_desc` | `string` | `varchar` |  |  | ORR_DESC |
| `orr_class` | `string` | `varchar` |  |  | ORR_CLASS |
| `orr_type` | `string` | `varchar` |  |  | ORR_TYPE |
| `orr_rtype` | `string` | `varchar` |  |  | ORR_RTYPE |
| `orr_supplier` | `string` | `varchar` |  |  | ORR_SUPPLIER |
| `orr_date` | `timestamp` | `timestamp_ntz` |  |  | ORR_DATE |
| `orr_due` | `timestamp` | `timestamp_ntz` |  |  | ORR_DUE |
| `orr_approv` | `timestamp` | `timestamp_ntz` |  |  | ORR_APPROV |
| `orr_status` | `string` | `varchar` |  |  | ORR_STATUS |
| `orr_rstatus` | `string` | `varchar` |  |  | ORR_RSTATUS |
| `orr_auth` | `string` | `varchar` |  |  | ORR_AUTH |
| `orr_contract` | `string` | `varchar` |  |  | ORR_CONTRACT |
| `orr_discperc` | `float` | `float` |  |  | ORR_DISCPERC |
| `orr_discount` | `float` | `float` |  |  | ORR_DISCOUNT |
| `orr_price` | `float` | `float` |  |  | ORR_PRICE |
| `orr_curr` | `string` | `varchar` |  |  | ORR_CURR |
| `orr_exch` | `float` | `float` |  |  | ORR_EXCH |
| `orr_origin` | `string` | `varchar` |  |  | ORR_ORIGIN |
| `orr_store` | `string` | `varchar` |  |  | ORR_STORE |
| `orr_printed` | `string` | `varchar` |  |  | ORR_PRINTED |
| `orr_buyer` | `string` | `varchar` |  |  | ORR_BUYER |
| `orr_mailtosite` | `string` | `varchar` |  |  | ORR_MAILTOSITE |
| `orr_paymentterms` | `string` | `varchar` |  |  | ORR_PAYMENTTERMS |
| `orr_freightterms` | `string` | `varchar` |  |  | ORR_FREIGHTTERMS |
| `orr_shipvia` | `string` | `varchar` |  |  | ORR_SHIPVIA |
| `orr_fobpoint` | `string` | `varchar` |  |  | ORR_FOBPOINT |
| `orr_blanketorder` | `string` | `varchar` |  |  | ORR_BLANKETORDER |
| `orr_dfltauth` | `string` | `varchar` |  |  | ORR_DFLTAUTH |
| `orr_exchtodual` | `float` | `float` |  |  | ORR_EXCHTODUAL |
| `orr_exchfromdual` | `float` | `float` |  |  | ORR_EXCHFROMDUAL |
| `orr_deladdress` | `string` | `varchar` |  |  | ORR_DELADDRESS |
| `orr_class_org` | `string` | `varchar` |  |  | ORR_CLASS_ORG |
| `orr_supplier_org` | `string` | `varchar` |  |  | ORR_SUPPLIER_ORG |
| `orr_order_org` | `string` | `varchar` | `PK` |  | ORR_ORDER_ORG |
| `orr_paybymethod` | `string` | `varchar` |  |  | ORR_PAYBYMETHOD |
| `orr_updatecount` | `float` | `float` |  |  | ORR_UPDATECOUNT |
| `orr_controlno` | `string` | `varchar` |  |  | ORR_CONTROLNO |
| `orr_blanketrelease` | `float` | `float` |  |  | ORR_BLANKETRELEASE |
| `orr_sourcecode` | `string` | `varchar` |  |  | ORR_SOURCECODE |
| `orr_iptransmitted` | `string` | `varchar` |  |  | ORR_IPTRANSMITTED |
| `orr_udfchar01` | `string` | `varchar` |  |  | ORR_UDFCHAR01 |
| `orr_udfchar02` | `string` | `varchar` |  |  | ORR_UDFCHAR02 |
| `orr_udfchar03` | `string` | `varchar` |  |  | ORR_UDFCHAR03 |
| `orr_udfchar04` | `string` | `varchar` |  |  | ORR_UDFCHAR04 |
| `orr_udfchar05` | `string` | `varchar` |  |  | ORR_UDFCHAR05 |
| `orr_udfchar06` | `string` | `varchar` |  |  | ORR_UDFCHAR06 |
| `orr_udfchar07` | `string` | `varchar` |  |  | ORR_UDFCHAR07 |
| `orr_udfchar08` | `string` | `varchar` |  |  | ORR_UDFCHAR08 |
| `orr_udfchar09` | `string` | `varchar` |  |  | ORR_UDFCHAR09 |
| `orr_udfchar10` | `string` | `varchar` |  |  | ORR_UDFCHAR10 |
| `orr_udfchar11` | `string` | `varchar` |  |  | ORR_UDFCHAR11 |
| `orr_udfchar12` | `string` | `varchar` |  |  | ORR_UDFCHAR12 |
| `orr_udfchar13` | `string` | `varchar` |  |  | ORR_UDFCHAR13 |
| `orr_udfchar14` | `string` | `varchar` |  |  | ORR_UDFCHAR14 |
| `orr_udfchar15` | `string` | `varchar` |  |  | ORR_UDFCHAR15 |
| `orr_udfchar16` | `string` | `varchar` |  |  | ORR_UDFCHAR16 |
| `orr_udfchar17` | `string` | `varchar` |  |  | ORR_UDFCHAR17 |
| `orr_udfchar18` | `string` | `varchar` |  |  | ORR_UDFCHAR18 |
| `orr_udfchar19` | `string` | `varchar` |  |  | ORR_UDFCHAR19 |
| `orr_udfchar20` | `string` | `varchar` |  |  | ORR_UDFCHAR20 |
| `orr_udfchar21` | `string` | `varchar` |  |  | ORR_UDFCHAR21 |
| `orr_udfchar22` | `string` | `varchar` |  |  | ORR_UDFCHAR22 |
| `orr_udfchar23` | `string` | `varchar` |  |  | ORR_UDFCHAR23 |
| `orr_udfchar24` | `string` | `varchar` |  |  | ORR_UDFCHAR24 |
| `orr_udfchar25` | `string` | `varchar` |  |  | ORR_UDFCHAR25 |
| `orr_udfchar26` | `string` | `varchar` |  |  | ORR_UDFCHAR26 |
| `orr_udfchar27` | `string` | `varchar` |  |  | ORR_UDFCHAR27 |
| `orr_udfchar28` | `string` | `varchar` |  |  | ORR_UDFCHAR28 |
| `orr_udfchar29` | `string` | `varchar` |  |  | ORR_UDFCHAR29 |
| `orr_udfchar30` | `string` | `varchar` |  |  | ORR_UDFCHAR30 |
| `orr_udfnum01` | `float` | `float` |  |  | ORR_UDFNUM01 |
| `orr_udfnum02` | `float` | `float` |  |  | ORR_UDFNUM02 |
| `orr_udfnum03` | `float` | `float` |  |  | ORR_UDFNUM03 |
| `orr_udfnum04` | `float` | `float` |  |  | ORR_UDFNUM04 |
| `orr_udfnum05` | `float` | `float` |  |  | ORR_UDFNUM05 |
| `orr_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | ORR_UDFDATE01 |
| `orr_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | ORR_UDFDATE02 |
| `orr_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | ORR_UDFDATE03 |
| `orr_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | ORR_UDFDATE04 |
| `orr_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | ORR_UDFDATE05 |
| `orr_udfchkbox01` | `string` | `varchar` |  |  | ORR_UDFCHKBOX01 |
| `orr_udfchkbox02` | `string` | `varchar` |  |  | ORR_UDFCHKBOX02 |
| `orr_udfchkbox03` | `string` | `varchar` |  |  | ORR_UDFCHKBOX03 |
| `orr_udfchkbox04` | `string` | `varchar` |  |  | ORR_UDFCHKBOX04 |
| `orr_udfchkbox05` | `string` | `varchar` |  |  | ORR_UDFCHKBOX05 |
| `orr_revisionreason` | `string` | `varchar` |  |  | ORR_REVISIONREASON |
| `orr_laststatusupdate` | `timestamp` | `timestamp_ntz` |  |  | ORR_LASTSTATUSUPDATE |
| `orr_packagetrackingnumber` | `string` | `varchar` |  |  | ORR_PACKAGETRACKINGNUMBER |
| `orr_sourcesystem` | `string` | `varchar` |  |  | ORR_SOURCESYSTEM |
| `orr_interface` | `timestamp` | `timestamp_ntz` |  |  | ORR_INTERFACE |
| `orr_created` | `timestamp` | `timestamp_ntz` |  |  | ORR_CREATED |
| `orr_updated` | `timestamp` | `timestamp_ntz` |  |  | ORR_UPDATED |
| `orr_acd` | `float` | `float` |  |  | ORR_ACD |
| `orr_jecategory` | `string` | `varchar` |  |  | ORR_JECATEGORY |
| `orr_creditcard` | `float` | `float` |  |  | ORR_CREDITCARD |
| `orr_jesource` | `string` | `varchar` |  |  | ORR_JESOURCE |
| `orr_attentionto` | `string` | `varchar` |  |  | ORR_ATTENTIONTO |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

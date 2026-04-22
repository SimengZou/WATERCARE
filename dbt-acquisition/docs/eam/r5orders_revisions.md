# r5orders_revisions

eam_R5ORDERS_REVISIONS

## Overview

| Property | Value |
|---|---|
| **Source system** | `eam` |
| **Source table** | `eam_r5orders_revisions` |
| **Materialization** | `incremental` |
| **Primary keys** | `ord_code`, `ord_org` |
| **Column count** | 108 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `ord_lastsaved` | `timestamp` | `timestamp_ntz` |  |  | ORD_LASTSAVED |
| `ord_add` | `string` | `varchar` |  |  | ORD_ADD |
| `ord_revision` | `float` | `float` |  |  | ORD_REVISION |
| `ord_act_rev` | `float` | `float` |  |  | ORD_ACT_REV |
| `ord_code` | `string` | `varchar` | `PK` |  | ORD_CODE |
| `ord_org` | `string` | `varchar` | `PK` |  | ORD_ORG |
| `ord_desc` | `string` | `varchar` |  |  | ORD_DESC |
| `ord_class` | `string` | `varchar` |  |  | ORD_CLASS |
| `ord_class_org` | `string` | `varchar` |  |  | ORD_CLASS_ORG |
| `ord_type` | `string` | `varchar` |  |  | ORD_TYPE |
| `ord_rtype` | `string` | `varchar` |  |  | ORD_RTYPE |
| `ord_supplier` | `string` | `varchar` |  |  | ORD_SUPPLIER |
| `ord_supplier_org` | `string` | `varchar` |  |  | ORD_SUPPLIER_ORG |
| `ord_date` | `timestamp` | `timestamp_ntz` |  |  | ORD_DATE |
| `ord_due` | `timestamp` | `timestamp_ntz` |  |  | ORD_DUE |
| `ord_approv` | `timestamp` | `timestamp_ntz` |  |  | ORD_APPROV |
| `ord_status` | `string` | `varchar` |  |  | ORD_STATUS |
| `ord_rstatus` | `string` | `varchar` |  |  | ORD_RSTATUS |
| `ord_auth` | `string` | `varchar` |  |  | ORD_AUTH |
| `ord_contract` | `string` | `varchar` |  |  | ORD_CONTRACT |
| `ord_discperc` | `float` | `float` |  |  | ORD_DISCPERC |
| `ord_discount` | `float` | `float` |  |  | ORD_DISCOUNT |
| `ord_price` | `float` | `float` |  |  | ORD_PRICE |
| `ord_curr` | `string` | `varchar` |  |  | ORD_CURR |
| `ord_exch` | `float` | `float` |  |  | ORD_EXCH |
| `ord_exchfromdual` | `float` | `float` |  |  | ORD_EXCHFROMDUAL |
| `ord_exchtodual` | `float` | `float` |  |  | ORD_EXCHTODUAL |
| `ord_origin` | `string` | `varchar` |  |  | ORD_ORIGIN |
| `ord_store` | `string` | `varchar` |  |  | ORD_STORE |
| `ord_printed` | `string` | `varchar` |  |  | ORD_PRINTED |
| `ord_buyer` | `string` | `varchar` |  |  | ORD_BUYER |
| `ord_paymentterms` | `string` | `varchar` |  |  | ORD_PAYMENTTERMS |
| `ord_freightterms` | `string` | `varchar` |  |  | ORD_FREIGHTTERMS |
| `ord_shipvia` | `string` | `varchar` |  |  | ORD_SHIPVIA |
| `ord_fobpoint` | `string` | `varchar` |  |  | ORD_FOBPOINT |
| `ord_dfltauth` | `string` | `varchar` |  |  | ORD_DFLTAUTH |
| `ord_blanketorder` | `string` | `varchar` |  |  | ORD_BLANKETORDER |
| `ord_deladdress` | `string` | `varchar` |  |  | ORD_DELADDRESS |
| `ord_paybymethod` | `string` | `varchar` |  |  | ORD_PAYBYMETHOD |
| `ord_creditcard` | `float` | `float` |  |  | ORD_CREDITCARD |
| `ord_controlno` | `string` | `varchar` |  |  | ORD_CONTROLNO |
| `ord_blanketrelease` | `float` | `float` |  |  | ORD_BLANKETRELEASE |
| `ord_sourcecode` | `string` | `varchar` |  |  | ORD_SOURCECODE |
| `ord_iptransmitted` | `string` | `varchar` |  |  | ORD_IPTRANSMITTED |
| `ord_updatecount` | `float` | `float` |  |  | ORD_UPDATECOUNT |
| `ord_revised` | `timestamp` | `timestamp_ntz` |  |  | ORD_REVISED |
| `ord_udfchar01` | `string` | `varchar` |  |  | ORD_UDFCHAR01 |
| `ord_udfchar02` | `string` | `varchar` |  |  | ORD_UDFCHAR02 |
| `ord_udfchar03` | `string` | `varchar` |  |  | ORD_UDFCHAR03 |
| `ord_udfchar04` | `string` | `varchar` |  |  | ORD_UDFCHAR04 |
| `ord_udfchar05` | `string` | `varchar` |  |  | ORD_UDFCHAR05 |
| `ord_udfchar06` | `string` | `varchar` |  |  | ORD_UDFCHAR06 |
| `ord_udfchar07` | `string` | `varchar` |  |  | ORD_UDFCHAR07 |
| `ord_udfchar08` | `string` | `varchar` |  |  | ORD_UDFCHAR08 |
| `ord_udfchar09` | `string` | `varchar` |  |  | ORD_UDFCHAR09 |
| `ord_udfchar10` | `string` | `varchar` |  |  | ORD_UDFCHAR10 |
| `ord_udfchar11` | `string` | `varchar` |  |  | ORD_UDFCHAR11 |
| `ord_udfchar12` | `string` | `varchar` |  |  | ORD_UDFCHAR12 |
| `ord_udfchar13` | `string` | `varchar` |  |  | ORD_UDFCHAR13 |
| `ord_udfchar14` | `string` | `varchar` |  |  | ORD_UDFCHAR14 |
| `ord_udfchar15` | `string` | `varchar` |  |  | ORD_UDFCHAR15 |
| `ord_udfchar16` | `string` | `varchar` |  |  | ORD_UDFCHAR16 |
| `ord_udfchar17` | `string` | `varchar` |  |  | ORD_UDFCHAR17 |
| `ord_udfchar18` | `string` | `varchar` |  |  | ORD_UDFCHAR18 |
| `ord_udfchar19` | `string` | `varchar` |  |  | ORD_UDFCHAR19 |
| `ord_udfchar20` | `string` | `varchar` |  |  | ORD_UDFCHAR20 |
| `ord_udfchar21` | `string` | `varchar` |  |  | ORD_UDFCHAR21 |
| `ord_udfchar22` | `string` | `varchar` |  |  | ORD_UDFCHAR22 |
| `ord_udfchar23` | `string` | `varchar` |  |  | ORD_UDFCHAR23 |
| `ord_udfchar24` | `string` | `varchar` |  |  | ORD_UDFCHAR24 |
| `ord_udfchar25` | `string` | `varchar` |  |  | ORD_UDFCHAR25 |
| `ord_udfchar26` | `string` | `varchar` |  |  | ORD_UDFCHAR26 |
| `ord_udfchar27` | `string` | `varchar` |  |  | ORD_UDFCHAR27 |
| `ord_udfchar28` | `string` | `varchar` |  |  | ORD_UDFCHAR28 |
| `ord_udfchar29` | `string` | `varchar` |  |  | ORD_UDFCHAR29 |
| `ord_udfchar30` | `string` | `varchar` |  |  | ORD_UDFCHAR30 |
| `ord_udfnum01` | `float` | `float` |  |  | ORD_UDFNUM01 |
| `ord_udfnum02` | `float` | `float` |  |  | ORD_UDFNUM02 |
| `ord_udfnum03` | `float` | `float` |  |  | ORD_UDFNUM03 |
| `ord_udfnum04` | `float` | `float` |  |  | ORD_UDFNUM04 |
| `ord_udfnum05` | `float` | `float` |  |  | ORD_UDFNUM05 |
| `ord_udfdate01` | `timestamp` | `timestamp_ntz` |  |  | ORD_UDFDATE01 |
| `ord_udfdate02` | `timestamp` | `timestamp_ntz` |  |  | ORD_UDFDATE02 |
| `ord_udfdate03` | `timestamp` | `timestamp_ntz` |  |  | ORD_UDFDATE03 |
| `ord_udfdate04` | `timestamp` | `timestamp_ntz` |  |  | ORD_UDFDATE04 |
| `ord_udfdate05` | `timestamp` | `timestamp_ntz` |  |  | ORD_UDFDATE05 |
| `ord_udfchkbox01` | `string` | `varchar` |  |  | ORD_UDFCHKBOX01 |
| `ord_udfchkbox02` | `string` | `varchar` |  |  | ORD_UDFCHKBOX02 |
| `ord_udfchkbox03` | `string` | `varchar` |  |  | ORD_UDFCHKBOX03 |
| `ord_udfchkbox04` | `string` | `varchar` |  |  | ORD_UDFCHKBOX04 |
| `ord_udfchkbox05` | `string` | `varchar` |  |  | ORD_UDFCHKBOX05 |
| `ord_revisionreason` | `string` | `varchar` |  |  | ORD_REVISIONREASON |
| `ord_sourcesystem` | `string` | `varchar` |  |  | ORD_SOURCESYSTEM |
| `ord_interface` | `timestamp` | `timestamp_ntz` |  |  | ORD_INTERFACE |
| `ord_created` | `timestamp` | `timestamp_ntz` |  |  | ORD_CREATED |
| `ord_updated` | `timestamp` | `timestamp_ntz` |  |  | ORD_UPDATED |
| `ord_acd` | `float` | `float` |  |  | ORD_ACD |
| `ord_jecategory` | `string` | `varchar` |  |  | ORD_JECATEGORY |
| `ord_jesource` | `string` | `varchar` |  |  | ORD_JESOURCE |
| `ord_attentionto` | `string` | `varchar` |  |  | ORD_ATTENTIONTO |
| `ord_laststatusupdate` | `timestamp` | `timestamp_ntz` |  |  | ORD_LASTSTATUSUPDATE |
| `ord_packagetrackingnumber` | `string` | `varchar` |  |  | ORD_PACKAGETRACKINGNUMBER |
| `_deleted` | `boolean` | `boolean` |  |  | Deleted Record Indicator |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

# tccom110

LN tccom110 Sold-to Business Partners table, Generated 2026-04-10T19:41:01Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tccom110` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ofbp` |
| **Column count** | 148 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ofbp` | `string` | `varchar` | `PK` | `not_null` | (required) Sold-to Business Partner. Max length: 9 |
| `bpst` | `integer` | `int` |  |  | Business Partner Status. Allowed values: 1, 2, 3, 254 |
| `bpst_kw` | `string` | `varchar` |  |  | Business Partner Status (keyword). Allowed values: tccom.prst.potential, tccom.prst.active, tccom.prst.inactive, tccom.prst.not.specified |
| `endt` | `timestamp` | `timestamp_ntz` |  |  | End Date |
| `stdt` | `timestamp` | `timestamp_ntz` |  |  | Start Date |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 3 |
| `creg` | `string` | `varchar` |  |  | Area. Max length: 3 |
| `ster` | `string` | `varchar` |  |  | Sales Territory. Max length: 6 |
| `cbrn` | `string` | `varchar` |  |  | Line of Business. Max length: 6 |
| `cbps` | `string` | `varchar` |  |  | Business Partner Signal. Max length: 3 |
| `cbtp` | `string` | `varchar` |  |  | Business Partner Type. Max length: 3 |
| `bpus` | `string` | `varchar` |  |  | Business Partner for Statistics Update. Max length: 9 |
| `cadr` | `string` | `varchar` |  |  | Address Code. Max length: 9 |
| `telp` | `string` | `varchar` |  |  | Phone Number. Max length: 32 |
| `tlcy` | `string` | `varchar` |  |  | Country for Phone Number. Max length: 6 |
| `tlci` | `string` | `varchar` |  |  | City/Area Code for Phone Number. Max length: 15 |
| `tlln` | `string` | `varchar` |  |  | Local Number for Phone Number. Max length: 32 |
| `tlex` | `string` | `varchar` |  |  | Extension for Phone Number. Max length: 15 |
| `tefx` | `string` | `varchar` |  |  | Fax Number. Max length: 32 |
| `tfcy` | `string` | `varchar` |  |  | Country for Fax Number. Max length: 6 |
| `tfci` | `string` | `varchar` |  |  | City/Area Code for Fax Number. Max length: 15 |
| `tfln` | `string` | `varchar` |  |  | Local Number for Fax Number. Max length: 32 |
| `tfex` | `string` | `varchar` |  |  | Extension for Fax Number. Max length: 15 |
| `ccal` | `string` | `varchar` |  |  | Calendar Code. Max length: 9 |
| `ccnt` | `string` | `varchar` |  |  | Contact. Max length: 9 |
| `crtc` | `string` | `varchar` |  |  | Response Type. Max length: 3 |
| `cspr` | `integer` | `int` |  |  | Priority |
| `mcfr` | `integer` | `int` |  |  | Rate Determiner. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `mcfr_kw` | `string` | `varchar` |  |  | Rate Determiner (keyword). Allowed values: tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `cpls` | `string` | `varchar` |  |  | Sales Price List. Max length: 3 |
| `pldd` | `string` | `varchar` |  |  | Price List for Direct Delivery. Max length: 3 |
| `odis` | `float` | `float` |  |  | Order Discount |
| `umsp` | `float` | `float` |  |  | Upper Margin |
| `lmsp` | `float` | `float` |  |  | Lower Margin |
| `bppr` | `string` | `varchar` |  |  | Business Partner for Prices/Discounts. Max length: 9 |
| `cacs` | `integer` | `int` |  |  | Calculate Additional Costs. Allowed values: 1, 2 |
| `cacs_kw` | `string` | `varchar` |  |  | Calculate Additional Costs (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cacf` | `integer` | `int` |  |  | Calculate Additional Costs For. Allowed values: 10, 20, 100, 254 |
| `cacf_kw` | `string` | `varchar` |  |  | Calculate Additional Costs For (keyword). Allowed values: tccom.cacf.orders, tccom.cacf.shipments, tccom.cacf.not.appl, tccom.cacf.not.specified |
| `macc` | `integer` | `int` |  |  | Method of Additional Cost Calculation. Allowed values: 10, 20, 100, 254 |
| `macc_kw` | `string` | `varchar` |  |  | Method of Additional Cost Calculation (keyword). Allowed values: tccom.macc.header.based, tccom.macc.line.based, tccom.macc.not.appl, tccom.macc.not.specified |
| `prms` | `integer` | `int` |  |  | Eligible for Promotion. Allowed values: 1, 2 |
| `prms_kw` | `string` | `varchar` |  |  | Eligible for Promotion (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aapr` | `integer` | `int` |  |  | Apply Automatic Promotions. Allowed values: 1, 2 |
| `aapr_kw` | `string` | `varchar` |  |  | Apply Automatic Promotions (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lcmp` | `integer` | `int` |  |  | Logistic Company |
| `cofc` | `string` | `varchar` |  |  | Sales Office. Max length: 6 |
| `crep` | `string` | `varchar` |  |  | Internal Sales Representative. Max length: 9 |
| `osrp` | `string` | `varchar` |  |  | External Sales Representative. Max length: 9 |
| `sotp` | `string` | `varchar` |  |  | Sales Order Type. Max length: 3 |
| `aprc` | `integer` | `int` |  |  | Automatically Process Received Customer Orders. Allowed values: 1, 2 |
| `aprc_kw` | `string` | `varchar` |  |  | Automatically Process Received Customer Orders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aaps` | `integer` | `int` |  |  | Automatically Approve Sales Orders. Allowed values: 1, 2 |
| `aaps_kw` | `string` | `varchar` |  |  | Automatically Approve Sales Orders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `osno` | `string` | `varchar` |  |  | Our Supplier Number. Max length: 15 |
| `clgr` | `string` | `varchar` |  |  | List Group. Max length: 3 |
| `aalg` | `integer` | `int` |  |  | Allow Alternative List Groups. Allowed values: 1, 2 |
| `aalg_kw` | `string` | `varchar` |  |  | Allow Alternative List Groups (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `chan` | `string` | `varchar` |  |  | Channel. Max length: 3 |
| `chak` | `integer` | `int` |  |  | EDI Order Change Response. Allowed values: 1, 2, 3, 254 |
| `chak_kw` | `string` | `varchar` |  |  | EDI Order Change Response (keyword). Allowed values: tccom.chak.order.ack, tccom.chak.order.chg.ack, tccom.chak.order.not.appl, tccom.chak.not.specified |
| `ackx` | `integer` | `int` |  |  | Acknowledge by Exception. Allowed values: 1, 2 |
| `ackx_kw` | `string` | `varchar` |  |  | Acknowledge by Exception (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ucnd` | `integer` | `int` |  |  | Use Confirmation. Allowed values: 1, 2 |
| `ucnd_kw` | `string` | `varchar` |  |  | Use Confirmation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sbil` | `integer` | `int` |  |  | Self-Billing. Allowed values: 1, 2 |
| `sbil_kw` | `string` | `varchar` |  |  | Self-Billing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pcmp` | `integer` | `int` |  |  | Price Component in use. Allowed values: 1, 2 |
| `pcmp_kw` | `string` | `varchar` |  |  | Price Component in use (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rinv` | `integer` | `int` |  |  | Receive Invoice. Allowed values: 1, 2 |
| `rinv_kw` | `string` | `varchar` |  |  | Receive Invoice (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `upsb` | `integer` | `int` |  |  | Update Price Based on SBI. Allowed values: 1, 2 |
| `upsb_kw` | `string` | `varchar` |  |  | Update Price Based on SBI (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pica` | `integer` | `int` |  |  | When Price inconsistent with Amount. Allowed values: 5, 10, 100, 254 |
| `pica_kw` | `string` | `varchar` |  |  | When Price inconsistent with Amount (keyword). Allowed values: tcsli.appr.prc.block.approval, tcsli.appr.prc.appr.without.pr, tcsli.appr.prc.not.applicable, tcsli.appr.prc.not.specified |
| `scon` | `integer` | `int` |  |  | Shipping Constraint. Allowed values: 1, 2, 3, 4, 5, 6, 10 |
| `scon_kw` | `string` | `varchar` |  |  | Shipping Constraint (keyword). Allowed values: tdscon.none, tdscon.sc, tdscon.rc, tdscon.oc, tdscon.sca, tdscon.kc, tdscon.not.applicable |
| `prio` | `integer` | `int` |  |  | Customer Priority |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `rdec` | `string` | `varchar` |  |  | Return Delivery Terms. Max length: 3 |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `scqq` | `integer` | `int` |  |  | Schedule Quantity Qualifier. Allowed values: 1, 2, 254 |
| `scqq_kw` | `string` | `varchar` |  |  | Schedule Quantity Qualifier (keyword). Allowed values: tccom.scqq.cum, tccom.scqq.ran, tccom.scqq.not.specified |
| `apsr` | `integer` | `int` |  |  | Automatically Process Sales Schedule Releases. Allowed values: 1, 2 |
| `apsr_kw` | `string` | `varchar` |  |  | Automatically Process Sales Schedule Releases (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `asma` | `integer` | `int` |  |  | Allow Shipment of Multiple Alternative Items. Allowed values: 1, 2 |
| `asma_kw` | `string` | `varchar` |  |  | Allow Shipment of Multiple Alternative Items (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `itbp` | `string` | `varchar` |  |  | Invoice-to Business Partner. Max length: 9 |
| `frin` | `integer` | `int` |  |  | Invoice Business Partner for Freight. Allowed values: 1, 2 |
| `frin_kw` | `string` | `varchar` |  |  | Invoice Business Partner for Freight (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `noem` | `integer` | `int` |  |  | Number of Employees |
| `arev` | `float` | `float` |  |  | Annual Revenue |
| `arcu` | `string` | `varchar` |  |  | Annual Revenue Currency. Max length: 3 |
| `incd` | `string` | `varchar` |  |  | Industry Code. Max length: 10 |
| `owsh__en_us` | `string` | `varchar` |  | `not_null` | (required) Ownership - base language. Max length: 30 |
| `rtng` | `integer` | `int` |  |  | Rating. Allowed values: 10, 20, 30, 40, 50, 254 |
| `rtng_kw` | `string` | `varchar` |  |  | Rating (keyword). Allowed values: tccom.rtng.none, tccom.rtng.bronze, tccom.rtng.silver, tccom.rtng.gold, tccom.rtng.platinum, tccom.rtng.not.specified |
| `user` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modification Date |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `mask` | `string` | `varchar` |  |  | Shipment Handling Unit Mask. Max length: 9 |
| `sscc` | `integer` | `int` |  |  | SSCC Standard. Allowed values: 1, 2 |
| `sscc_kw` | `string` | `varchar` |  |  | SSCC Standard (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Text |
| `ofbp_stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom117 Ship-to by Sold-to Business Partner |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `creg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs045 Areas |
| `ster_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs127 Sales Territories |
| `cbrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs031 Lines of Business |
| `cbps_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs039 Signals |
| `cbtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs029 Business Partner Types |
| `bpus_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `ccnt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `cspr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs070 Priorities |
| `cpls_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs034 Price Lists |
| `pldd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs034 Price Lists |
| `bppr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `lcmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `crep_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `osrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `clgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd301 List Groups |
| `chan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs066 Channels |
| `prio_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs070 Priorities |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `rdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `arcu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `incd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs125 Industry Codes |
| `mask_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd402 Masks |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `deleted` | `boolean` | `boolean` |  | `not_null` | (required) Whether record is deleted |
| `username` | `string` | `varchar` |  | `not_null` | (required) User responsible for record action. Max length: 8 |
| `timestamp` | `timestamp` | `timestamp_ntz` |  | `not_null` | (required) Time the action occurred |
| `sequencenumber` | `integer` | `int` |  | `not_null` | (required) Sequence number of the action |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

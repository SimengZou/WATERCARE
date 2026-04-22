# tdpur301

LN tdpur301 Purchase Contract Lines table, Generated 2026-04-10T19:41:20Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur301` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono`, `pono`, `cofc`, `csqn` |
| **Column count** | 228 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `string` | `varchar` | `PK` | `not_null` | (required) Contract. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `cofc` | `string` | `varchar` | `PK` | `not_null` | (required) Purchase Office. Max length: 6 |
| `csqn` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `poff` | `string` | `varchar` |  |  | Purchase Office. Max length: 6 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cltp` | `integer` | `int` |  |  | Contract Line Type. Allowed values: 1, 2, 3 |
| `cltp_kw` | `string` | `varchar` |  |  | Contract Line Type (keyword). Allowed values: tdgen.cltp.total, tdgen.cltp.detail, tdgen.cltp.contract.line |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `sfad` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `sfcn` | `string` | `varchar` |  |  | Ship-from Contact. Max length: 9 |
| `icap` | `integer` | `int` |  |  | Contract Line Status. Allowed values: 1, 2, 3 |
| `icap_kw` | `string` | `varchar` |  |  | Contract Line Status (keyword). Allowed values: tcicap.free, tcicap.active, tcicap.closed |
| `icyp` | `integer` | `int` |  |  | Contract Type. Allowed values: 1, 2 |
| `icyp_kw` | `string` | `varchar` |  |  | Contract Type (keyword). Allowed values: tcicyp.year, tcicyp.proj |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `crrf` | `integer` | `int` |  |  | Item Cross Reference. Allowed values: 1, 2, 3 |
| `crrf_kw` | `string` | `varchar` |  |  | Item Cross Reference (keyword). Allowed values: tccrrf.ics, tccrrf.mpn, tccrrf.na |
| `citt` | `string` | `varchar` |  |  | Code Item Type. Max length: 3 |
| `crit__en_us` | `string` | `varchar` |  | `not_null` | (required) Cross Reference Item - base language. Max length: 35 |
| `mpnr` | `string` | `varchar` |  |  | Manufacturer Part Number. Max length: 35 |
| `cmnf` | `string` | `varchar` |  |  | Manufacturer. Max length: 6 |
| `acqm` | `integer` | `int` |  |  | Acquisition Method. Allowed values: 5, 10, 99 |
| `acqm_kw` | `string` | `varchar` |  |  | Acquisition Method (keyword). Allowed values: tcacqm.buying, tcacqm.rental, tcacqm.not.appl |
| `subc` | `integer` | `int` |  |  | Subcontracted. Allowed values: 1, 2, 3 |
| `subc_kw` | `string` | `varchar` |  |  | Subcontracted (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `subs` | `string` | `varchar` |  |  | Subcontractor Site. Max length: 9 |
| `ncmp` | `integer` | `int` |  |  | Company |
| `rcsi` | `string` | `varchar` |  |  | Receiving Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `cadr` | `string` | `varchar` |  |  | Warehouse Address. Max length: 9 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `sdsc` | `integer` | `int` |  |  | Standard Description. Allowed values: 1, 2 |
| `sdsc_kw` | `string` | `varchar` |  |  | Standard Description (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cpgp` | `string` | `varchar` |  |  | Purchase Price Group. Max length: 6 |
| `cdat` | `timestamp` | `timestamp_ntz` |  |  | Contract Date |
| `sdat` | `timestamp` | `timestamp_ntz` |  |  | Effective Date |
| `edat` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `leng` | `float` | `float` |  |  | Length |
| `widt` | `float` | `float` |  |  | Width |
| `thic` | `float` | `float` |  |  | Height |
| `qoor` | `float` | `float` |  |  | Agreed Quantity |
| `qogo` | `float` | `float` |  |  | Agreed Quantity |
| `qomn` | `float` | `float` |  |  | Minimum Quantity |
| `qomx` | `float` | `float` |  |  | Maximum Quantity |
| `aemq` | `integer` | `int` |  |  | Action on Exceeding Maximum Quantity. Allowed values: 1, 2, 3 |
| `aemq_kw` | `string` | `varchar` |  |  | Action on Exceeding Maximum Quantity (keyword). Allowed values: tdpur.imae.take.contract, tdpur.imae.ask.for.it, tdpur.imae.skip.contract |
| `mqtl` | `float` | `float` |  |  | Maximum Quantity Tolerance |
| `iqab` | `integer` | `int` |  |  | Quantity Binding. Allowed values: 1, 2 |
| `iqab_kw` | `string` | `varchar` |  |  | Quantity Binding (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cuqp` | `string` | `varchar` |  |  | Purchase Unit. Max length: 3 |
| `cvqp` | `float` | `float` |  |  | Conversion Factor Purchase to Inventory Unit |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `qiap` | `float` | `float` |  |  | Advised Quantity |
| `qicl` | `float` | `float` |  |  | Called Quantity |
| `camt` | `float` | `float` |  |  | Called Amount |
| `qiiv` | `float` | `float` |  |  | Invoiced Quantity |
| `bamt` | `float` | `float` |  |  | Invoiced Amount |
| `qigc` | `float` | `float` |  |  | Called Quantity |
| `caga` | `float` | `float` |  |  | Called Amount |
| `qigi` | `float` | `float` |  |  | Invoiced Quantity |
| `iaga` | `float` | `float` |  |  | Invoiced Amount |
| `ieva` | `integer` | `int` |  |  | Evaluation [y/n/repeat]. Allowed values: 1, 2, 3 |
| `ieva_kw` | `string` | `varchar` |  |  | Evaluation [y/n/repeat] (keyword). Allowed values: tcieva.yes, tcieva.no, tcieva.repeat |
| `revi` | `string` | `varchar` |  |  | Engineering Item. Max length: 6 |
| `ceno` | `string` | `varchar` |  |  | Exempt Certificate. Max length: 24 |
| `owno__en_us` | `string` | `varchar` |  | `not_null` | (required) BP Order Reference - base language. Max length: 30 |
| `rcod` | `string` | `varchar` |  |  | Exempt Reason. Max length: 6 |
| `bptc` | `string` | `varchar` |  |  | Business Partner Tax Country. Max length: 3 |
| `exmt` | `integer` | `int` |  |  | Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `csts` | `integer` | `int` |  |  | Print Status. Allowed values: 1, 2, 3, 4, 5, 6 |
| `csts_kw` | `string` | `varchar` |  |  | Print Status (keyword). Allowed values: tccsts.printed, tccsts.not.printed, tccsts.changed, tccsts.draft, tccsts.original, tccsts.changes.printed |
| `sbim` | `integer` | `int` |  |  | Self-Billing. Allowed values: 1, 2 |
| `sbim_kw` | `string` | `varchar` |  |  | Self-Billing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `paft` | `integer` | `int` |  |  | Invoice after. Allowed values: 1, 2 |
| `paft_kw` | `string` | `varchar` |  |  | Invoice after (keyword). Allowed values: tcpaft.approval, tcpaft.receipts |
| `sbdt` | `integer` | `int` |  |  | Self-Billing Date Type. Allowed values: 10, 20 |
| `sbdt_kw` | `string` | `varchar` |  |  | Self-Billing Date Type (keyword). Allowed values: tcsbdt.receipt.date, tcsbdt.shipping.date |
| `sbmt` | `string` | `varchar` |  |  | Self-Billing Method. Max length: 3 |
| `rpdr` | `integer` | `int` |  |  | Redetermine Price and Discounts at Receipt. Allowed values: 1, 2 |
| `rpdr_kw` | `string` | `varchar` |  |  | Redetermine Price and Discounts at Receipt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `obpr` | `integer` | `int` |  |  | Option Based Pricing. Allowed values: 1, 2 |
| `obpr_kw` | `string` | `varchar` |  |  | Option Based Pricing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `scus` | `integer` | `int` |  |  | Purchase Schedule in use. Allowed values: 1, 2 |
| `scus_kw` | `string` | `varchar` |  |  | Purchase Schedule in use (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `qual` | `integer` | `int` |  |  | Inspection. Allowed values: 1, 2 |
| `qual_kw` | `string` | `varchar` |  |  | Inspection (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `cons` | `integer` | `int` |  |  | Consigned. Allowed values: 1, 2 |
| `cons_kw` | `string` | `varchar` |  |  | Consigned (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `qimf` | `float` | `float` |  |  | Order Quantity Increment |
| `qimi` | `float` | `float` |  |  | Minimum Order Quantity |
| `qima` | `float` | `float` |  |  | Maximum Order Quantity |
| `qiec` | `float` | `float` |  |  | Economic Order Quantity |
| `qifi` | `float` | `float` |  |  | Fixed Order Quantity |
| `ccon` | `string` | `varchar` |  |  | Buyer. Max length: 9 |
| `dsch` | `integer` | `int` |  |  | Delivery Contract Available. Allowed values: 1, 2 |
| `dsch_kw` | `string` | `varchar` |  |  | Delivery Contract Available (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `txtt` | `integer` | `int` |  |  | Termination text |
| `txta` | `integer` | `int` |  |  | Contract Line Text |
| `cdf_cdsc__en_us` | `string` | `varchar` |  | `not_null` | (required) Sub-Category Description - base language. Max length: 250 |
| `cdf_clmt` | `float` | `float` |  |  | Contract Line Net Amount |
| `cdf_comt` | `float` | `float` |  |  | Contract Line Consumed Amount |
| `cdf_idet` | `string` | `varchar` |  |  | Item Detail. Max length: 100 |
| `cdf_ramt` | `float` | `float` |  |  | Contract Line Remaining Amount |
| `cdf_refe__en_us` | `string` | `varchar` |  | `not_null` | (required) Procurement Number - base language. Max length: 30 |
| `cdf_sctg` | `string` | `varchar` |  |  | Sub-Category. Max length: 25 |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur300 Purchase Contracts |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur012 Purchase Offices |
| `poff_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur012 Purchase Offices |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom121 Ship-from Business Partners |
| `sfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `sfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu001 Item - Purchase |
| `citt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd006 Item Code Systems |
| `mpnr_cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu045 Manufacturer Part Numbers |
| `cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs060 Manufacturers |
| `subs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `ncmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `rcsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `cpgp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs024 Price Groups |
| `cuqp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `ccty_cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs036 Tax Codes by Country |
| `rcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `sbmt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs057 Self-Billing Methods |
| `ccon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `txtt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `qoor_invu` | `float` | `float` |  |  | CC: Agreed Quantity in Inventory Unit |
| `qoor_buwg` | `float` | `float` |  |  | CC: Agreed Quantity in Base Unit Weight |
| `qoor_buln` | `float` | `float` |  |  | CC: Agreed Quantity in Base Unit Length |
| `qoor_buar` | `float` | `float` |  |  | CC: Agreed Quantity in Base Unit Area |
| `qoor_buvl` | `float` | `float` |  |  | CC: Agreed Quantity in Base Unit Volume |
| `qoor_butm` | `float` | `float` |  |  | CC: Agreed Quantity in Base Unit Time |
| `qoor_bupc` | `float` | `float` |  |  | CC: Agreed Quantity in Base Unit Piece |
| `qicl_trnu` | `float` | `float` |  |  | CC: Called Quantity in Transaction Unit |
| `qicl_buwg` | `float` | `float` |  |  | CC: Called Quantity in Base Unit Weight |
| `qicl_buln` | `float` | `float` |  |  | CC: Called Quantity in Base Unit Length |
| `qicl_buar` | `float` | `float` |  |  | CC: Called Quantity in Base Unit Area |
| `qicl_buvl` | `float` | `float` |  |  | CC: Called Quantity in Base Unit Volume |
| `qicl_butm` | `float` | `float` |  |  | CC: Called Quantity in Base Unit Time |
| `qicl_bupc` | `float` | `float` |  |  | CC: Called Quantity in Base Unit Piece |
| `qiiv_trnu` | `float` | `float` |  |  | CC: Invoiced Quantity in Transaction Unit |
| `qiiv_buwg` | `float` | `float` |  |  | CC: Invoiced Quantity in Base Unit Weight |
| `qiiv_buln` | `float` | `float` |  |  | CC: Invoiced Quantity in Base Unit Length |
| `qiiv_buar` | `float` | `float` |  |  | CC: Invoiced Quantity in Base Unit Area |
| `qiiv_buvl` | `float` | `float` |  |  | CC: Invoiced Quantity in Base Unit Volume |
| `qiiv_butm` | `float` | `float` |  |  | CC: Invoiced Quantity in Base Unit Time |
| `qiiv_bupc` | `float` | `float` |  |  | CC: Invoiced Quantity in Base Unit Piece |
| `camt_lclc` | `float` | `float` |  |  | CC: Called Amount in Local Currency |
| `camt_rpc1` | `float` | `float` |  |  | CC: Called Amount in Reporting Currency 1 |
| `camt_rpc2` | `float` | `float` |  |  | CC: Called Amount in Reporting Currency 2 |
| `camt_rfrc` | `float` | `float` |  |  | CC: Called Amount in Reference Currency |
| `camt_dtwc` | `float` | `float` |  |  | CC: Called Amount in Data Warehouse Currency |
| `bamt_lclc` | `float` | `float` |  |  | CC: Invoiced Amount in Local Currency |
| `bamt_rpc1` | `float` | `float` |  |  | CC: Invoiced Amount in Reporting Currency 1 |
| `bamt_rpc2` | `float` | `float` |  |  | CC: Invoiced Amount in Reporting Currency 2 |
| `bamt_rfrc` | `float` | `float` |  |  | CC: Invoiced Amount in Reference Currency |
| `bamt_dtwc` | `float` | `float` |  |  | CC: Invoiced Amount in Data Warehouse Currency |
| `qogo_invu` | `float` | `float` |  |  | CC: Agreed Quantity (Total) in Inventory Unit |
| `qogo_buwg` | `float` | `float` |  |  | CC: Agreed Quantity (Total) in Base Unit Weight |
| `qogo_buln` | `float` | `float` |  |  | CC: Agreed Quantity (Total) in Base Unit Length |
| `qogo_buar` | `float` | `float` |  |  | CC: Agreed Quantity (Total) in Base Unit Area |
| `qogo_buvl` | `float` | `float` |  |  | CC: Agreed Quantity (Total) in Base Unit Volume |
| `qogo_butm` | `float` | `float` |  |  | CC: Agreed Quantity (Total) in Base Unit Time |
| `qogo_bupc` | `float` | `float` |  |  | CC: Agreed Quantity (Total) in Base Unit Piece |
| `qigc_trnu` | `float` | `float` |  |  | CC: Called Quantity (Total) in Transaction Unit |
| `qigc_buwg` | `float` | `float` |  |  | CC: Called Quantity (Total) in Base Unit Weight |
| `qigc_buln` | `float` | `float` |  |  | CC: Called Quantity (Total) in Base Unit Length |
| `qigc_buar` | `float` | `float` |  |  | CC: Called Quantity (Total) in Base Unit Area |
| `qigc_buvl` | `float` | `float` |  |  | CC: Called Quantity (Total) in Base Unit Volume |
| `qigc_butm` | `float` | `float` |  |  | CC: Called Quantity (Total) in Base Unit Time |
| `qigc_bupc` | `float` | `float` |  |  | CC: Called Quantity (Total) in Base Unit Piece |
| `qigi_trnu` | `float` | `float` |  |  | CC: Invoiced Quantity (Total) in Transaction Unit |
| `qigi_buwg` | `float` | `float` |  |  | CC: Invoiced Quantity (Total) in Base Unit Weight |
| `qigi_buln` | `float` | `float` |  |  | CC: Invoiced Quantity (Total) in Base Unit Length |
| `qigi_buar` | `float` | `float` |  |  | CC: Invoiced Quantity (Total) in Base Unit Area |
| `qigi_buvl` | `float` | `float` |  |  | CC: Invoiced Quantity (Total) in Base Unit Volume |
| `qigi_butm` | `float` | `float` |  |  | CC: Invoiced Quantity (Total) in Base Unit Time |
| `qigi_bupc` | `float` | `float` |  |  | CC: Invoiced Quantity (Total) in Base Unit Piece |
| `caga_lclc` | `float` | `float` |  |  | CC: Called Amount (Total) in Local Currency |
| `caga_rpc1` | `float` | `float` |  |  | CC: Called Amount (Total) in Reporting Currency 1 |
| `caga_rpc2` | `float` | `float` |  |  | CC: Called Amount (Total) in Reporting Currency 2 |
| `caga_rfrc` | `float` | `float` |  |  | CC: Called Amount (Total) in Reference Currency |
| `caga_dtwc` | `float` | `float` |  |  | CC: Called Amount (Total) in Data Warehouse Currency |
| `iaga_lclc` | `float` | `float` |  |  | CC: Invoiced Amount (Total) in Local Currency |
| `iaga_rpc1` | `float` | `float` |  |  | CC: Invoiced Amount (Total) in Reporting Currency 1 |
| `iaga_rpc2` | `float` | `float` |  |  | CC: Invoiced Amount (Total) in Reporting Currency 2 |
| `iaga_rfrc` | `float` | `float` |  |  | CC: Invoiced Amount (Total) in Reference Currency |
| `iaga_dtwc` | `float` | `float` |  |  | CC: Invoiced Amount (Total) in Data Warehouse Currency |
| `gamt_trnc` | `float` | `float` |  |  | CC: Gross Amount in Transaction Currency |
| `gamt_lclc` | `float` | `float` |  |  | CC: Gross Amount in Local Currency |
| `gamt_rpc1` | `float` | `float` |  |  | CC: Gross Amount in Reporting Currency 1 |
| `gamt_rpc2` | `float` | `float` |  |  | CC: Gross Amount in Reporting Currency 2 |
| `gamt_rfrc` | `float` | `float` |  |  | CC: Gross Amount in Reference Currency |
| `gamt_dtwc` | `float` | `float` |  |  | CC: Gross Amount in Data Warehouse Currency |
| `namt_trnc` | `float` | `float` |  |  | CC: Net Amount in Transaction Currency |
| `namt_lclc` | `float` | `float` |  |  | CC: Net Amount in Local Currency |
| `namt_rpc1` | `float` | `float` |  |  | CC: Net Amount in Reporting Currency 1 |
| `namt_rpc2` | `float` | `float` |  |  | CC: Net Amount in Reporting Currency 2 |
| `namt_rfrc` | `float` | `float` |  |  | CC: Net Amount in Reference Currency |
| `namt_dtwc` | `float` | `float` |  |  | CC: Net Amount in Data Warehouse Currency |
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

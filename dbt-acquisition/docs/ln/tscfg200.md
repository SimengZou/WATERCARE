# tscfg200

LN tscfg200 Serialized Items table, Generated 2026-04-10T19:42:28Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tscfg200` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `item`, `sern` |
| **Column count** | 193 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `sern` | `string` | `varchar` | `PK` | `not_null` | (required) Serial Number. Max length: 30 |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 80 |
| `sear__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Argument - base language. Max length: 16 |
| `dsnr` | `integer` | `int` |  |  | Dummy Serial Number. Allowed values: 1, 2 |
| `dsnr_kw` | `string` | `varchar` |  |  | Dummy Serial Number (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `srbp__en_us` | `string` | `varchar` |  | `not_null` | (required) Alternate Serial Number - base language. Max length: 30 |
| `crtm` | `timestamp` | `timestamp_ntz` |  |  | Creation Time |
| `dscb__en_us` | `string` | `varchar` |  | `not_null` | (required) Material - base language. Max length: 30 |
| `dscc__en_us` | `string` | `varchar` |  | `not_null` | (required) Size - base language. Max length: 30 |
| `dscd__en_us` | `string` | `varchar` |  | `not_null` | (required) Standard - base language. Max length: 30 |
| `wght` | `float` | `float` |  |  | Weight |
| `mobl` | `integer` | `int` |  |  | Mobile. Allowed values: 1, 2 |
| `mobl_kw` | `string` | `varchar` |  |  | Mobile (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sigr` | `string` | `varchar` |  |  | Serialized Item Group. Max length: 6 |
| `soff` | `string` | `varchar` |  |  | Service Office. Max length: 6 |
| `rste` | `string` | `varchar` |  |  | Repair Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `lste` | `string` | `varchar` |  |  | Location Site. Max length: 9 |
| `ofbp` | `string` | `varchar` |  |  | Owner. Max length: 9 |
| `ofad` | `string` | `varchar` |  |  | Owner Address. Max length: 9 |
| `ofcn` | `string` | `varchar` |  |  | Owner Contact. Max length: 9 |
| `ubbp` | `string` | `varchar` |  |  | In Use-by Business Partner. Max length: 9 |
| `ubcn` | `string` | `varchar` |  |  | In Use-by Business Partner Contact. Max length: 9 |
| `ubad` | `string` | `varchar` |  |  | In Use-by Business Partner Address. Max length: 9 |
| `ubop` | `string` | `varchar` |  |  | In Use-by Operator Contact. Max length: 9 |
| `ubpc` | `integer` | `int` |  |  | In Use-by Primary Contact. Allowed values: 10, 20 |
| `ubpc_kw` | `string` | `varchar` |  |  | In Use-by Primary Contact (keyword). Allowed values: tscfg.pcnt.in.use.by, tscfg.pcnt.operator |
| `dler` | `string` | `varchar` |  |  | Dealer. Max length: 9 |
| `dlad` | `string` | `varchar` |  |  | Dealer Address. Max length: 9 |
| `dlcn` | `string` | `varchar` |  |  | Dealer Contact. Max length: 9 |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `bfad` | `string` | `varchar` |  |  | Buy-from Address. Max length: 9 |
| `bfcn` | `string` | `varchar` |  |  | Buy-from Contact. Max length: 9 |
| `owtp` | `integer` | `int` |  |  | Type of Ownership. Allowed values: 1, 2 |
| `owtp_kw` | `string` | `varchar` |  |  | Type of Ownership (keyword). Allowed values: tscfg.owtp.external, tscfg.owtp.internal |
| `dbpo` | `integer` | `int` |  |  | Default Business Partner for Order. Allowed values: 1, 2, 3, 4 |
| `dbpo_kw` | `string` | `varchar` |  |  | Default Business Partner for Order (keyword). Allowed values: tscfg.dbpo.owner, tscfg.dbpo.in.use.by, tscfg.dbpo.dealer, tscfg.dbpo.supplier |
| `sswt` | `integer` | `int` |  |  | Serialized Item Warranty Terms. Allowed values: 1, 2 |
| `sswt_kw` | `string` | `varchar` |  |  | Serialized Item Warranty Terms (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cwte` | `string` | `varchar` |  |  | Warranty. Max length: 16 |
| `term` | `integer` | `int` |  |  | Term ID |
| `swte` | `string` | `varchar` |  |  | Supplier Warranty. Max length: 16 |
| `strm` | `integer` | `int` |  |  | Supplier Warranty Term ID |
| `gwte` | `string` | `varchar` |  |  | Generic Warranty. Max length: 16 |
| `optm` | `timestamp` | `timestamp_ntz` |  |  | Installation Time |
| `prio` | `integer` | `int` |  |  | Priority |
| `cusc` | `string` | `varchar` |  |  | Usage Class. Max length: 3 |
| `ccal` | `string` | `varchar` |  |  | Calendar. Max length: 9 |
| `ract` | `string` | `varchar` |  |  | Availability Type. Max length: 6 |
| `cvco` | `integer` | `int` |  |  | Covered by Contract. Allowed values: 5, 10, 15 |
| `cvco_kw` | `string` | `varchar` |  |  | Covered by Contract (keyword). Allowed values: tscfg.cvco.covered, tscfg.cvco.covdef, tscfg.cvco.notcover |
| `revi` | `string` | `varchar` |  |  | Revision. Max length: 6 |
| `prip` | `float` | `float` |  |  | Sales Value |
| `dltm` | `timestamp` | `timestamp_ntz` |  |  | Delivery Time |
| `cpro` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `pelt` | `string` | `varchar` |  |  | Project Element/Activity. Max length: 16 |
| `ortp` | `integer` | `int` |  |  | Originating Order Type. Allowed values: 5, 10, 15, 99 |
| `ortp_kw` | `string` | `varchar` |  |  | Originating Order Type (keyword). Allowed values: tscfg.ortp.sales, tscfg.ortp.deliverable, tscfg.ortp.project, tscfg.ortp.not.applicable |
| `srno` | `string` | `varchar` |  |  | Originating Order. Max length: 9 |
| `cnln` | `string` | `varchar` |  |  | Originating Contract Line. Max length: 8 |
| `spno` | `integer` | `int` |  |  | Originating Order Line |
| `sdno` | `integer` | `int` |  |  | Originating Detail Line |
| `spfa__en_us` | `string` | `varchar` |  | `not_null` | (required) Specification A - base language. Max length: 20 |
| `spfb__en_us` | `string` | `varchar` |  | `not_null` | (required) Specification B - base language. Max length: 20 |
| `cmnf` | `string` | `varchar` |  |  | Manufacturer. Max length: 6 |
| `mftm` | `timestamp` | `timestamp_ntz` |  |  | Manufacturing Date |
| `pste` | `string` | `varchar` |  |  | Production Site. Max length: 9 |
| `cwoc` | `string` | `varchar` |  |  | Work Center. Max length: 6 |
| `mcno` | `string` | `varchar` |  |  | Machine Type. Max length: 9 |
| `mcnr` | `string` | `varchar` |  |  | Machine Number. Max length: 15 |
| `wcel` | `string` | `varchar` |  |  | Work Cell. Max length: 6 |
| `wstt` | `string` | `varchar` |  |  | Work Station Machine. Max length: 6 |
| `clst` | `string` | `varchar` |  |  | Installation Group. Max length: 9 |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 1, 2, 3, 4, 5, 6, 9, 15, 99 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tscfg.cfst.stup, tscfg.cfst.actv, tscfg.cfst.revi, tscfg.cfst.dftv, tscfg.cfst.wkcn, tscfg.cfst.tbrc, tscfg.cfst.removed, tscfg.cfst.superseded, tscfg.cfst.nota |
| `sttm` | `timestamp` | `timestamp_ntz` |  |  | Status Time |
| `sttp` | `integer` | `int` |  |  | Structure Type. Allowed values: 5, 15 |
| `sttp_kw` | `string` | `varchar` |  |  | Structure Type (keyword). Allowed values: tscfg.sttp.structure, tscfg.sttp.physical |
| `titm` | `string` | `varchar` |  |  | Top Item. Max length: 47 |
| `tser` | `string` | `varchar` |  |  | Top Serial Number. Max length: 30 |
| `phyt` | `integer` | `int` |  |  | Physical Top. Allowed values: 1, 2 |
| `phyt_kw` | `string` | `varchar` |  |  | Physical Top (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ladr` | `string` | `varchar` |  |  | Location Address. Max length: 9 |
| `linf__en_us` | `string` | `varchar` |  | `not_null` | (required) Location Information - base language. Max length: 30 |
| `ccnt` | `string` | `varchar` |  |  | Technical Contact. Max length: 9 |
| `trdt` | `float` | `float` |  |  | Travel Distance |
| `trtm` | `float` | `float` |  |  | Travel Time |
| `ccha` | `float` | `float` |  |  | Call-out Charge |
| `ccur` | `string` | `varchar` |  |  | Call-out Charge Currency. Max length: 3 |
| `csar` | `string` | `varchar` |  |  | Service Area. Max length: 3 |
| `czon` | `string` | `varchar` |  |  | Distance Zone. Max length: 3 |
| `mdpt` | `string` | `varchar` |  |  | Operations Department. Max length: 6 |
| `emno` | `string` | `varchar` |  |  | Preferred Engineer Field Service. Max length: 9 |
| `prfb` | `string` | `varchar` |  |  | Preferred Engineer Depot Repair. Max length: 9 |
| `prop` | `string` | `varchar` |  |  | Preferred Operator. Max length: 9 |
| `sbct` | `string` | `varchar` |  |  | Subcontractor. Max length: 9 |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `shad` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `blag__en_us` | `string` | `varchar` |  | `not_null` | (required) Blanket Agreement - base language. Max length: 30 |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `rwdu` | `integer` | `int` |  |  | Repair Warranty Duration |
| `rwun` | `integer` | `int` |  |  | Repair Warranty Duration Unit. Allowed values: 0, 5, 10, 15, 20, 25 |
| `rwun_kw` | `string` | `varchar` |  |  | Repair Warranty Duration Unit (keyword). Allowed values: , tsmdm.peru.day, tsmdm.peru.week, tsmdm.peru.month, tsmdm.peru.quarter, tsmdm.peru.year |
| `alty` | `integer` | `int` |  |  | Actual Location Type. Allowed values: 5, 15, 17, 20, 25, 30, 35, 45 |
| `alty_kw` | `string` | `varchar` |  |  | Actual Location Type (keyword). Allowed values: tscfg.actl.configuration, tscfg.actl.warehouse, tscfg.actl.location, tscfg.actl.repairshop, tscfg.actl.transit, tscfg.actl.loaned, tscfg.actl.rented.out, tscfg.actl.not.appl |
| `aloc` | `string` | `varchar` |  |  | Actual Location. Max length: 6 |
| `lutm` | `timestamp` | `timestamp_ntz` |  |  | Last Updated Time |
| `pltm` | `timestamp` | `timestamp_ntz` |  |  | Planned Until Time |
| `tery` | `string` | `varchar` |  |  | Territory. Max length: 8 |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `lpnr__en_us` | `string` | `varchar` |  | `not_null` | (required) License Plate Number - base language. Max length: 50 |
| `gpac` | `integer` | `int` |  |  | Generate Planned Activities. Allowed values: 1, 2 |
| `gpac_kw` | `string` | `varchar` |  |  | Generate Planned Activities (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pmsc` | `string` | `varchar` |  |  | Preventive Maintenance Scenario. Max length: 9 |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `pbsm` | `integer` | `int` |  |  | Problem Solving Method. Allowed values: 10, 20, 30, 90, 100 |
| `pbsm_kw` | `string` | `varchar` |  |  | Problem Solving Method (keyword). Allowed values: tcmcs.pbsm.8d, tcmcs.pbsm.a3, tcmcs.pbsm.chlt, tcmcs.pbsm.other, tcmcs.pbsm.na |
| `rnyn` | `integer` | `int` |  |  | Rentable. Allowed values: 1, 2 |
| `rnyn_kw` | `string` | `varchar` |  |  | Rentable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rown` | `string` | `varchar` |  |  | Rental Owner. Max length: 6 |
| `rowc` | `integer` | `int` |  |  | Rental Owner Company |
| `tgps` | `integer` | `int` |  |  | Track GPS Location. Allowed values: 1, 2 |
| `tgps_kw` | `string` | `varchar` |  |  | Track GPS Location (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cmea` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `cndt` | `timestamp` | `timestamp_ntz` |  |  | Obsolete |
| `coun` | `float` | `float` |  |  | Obsolete |
| `txta` | `integer` | `int` |  |  | General Text |
| `txtb` | `integer` | `int` |  |  | Technical Text |
| `txtc` | `integer` | `int` |  |  | Specification Text |
| `item_sern_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd401 Serial Numbers |
| `item_lste_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd150 Items by Site |
| `item_rste_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd150 Items by Site |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm200 Items - Service |
| `sigr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tscfg010 Serialized Item Groups |
| `soff_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm100 Service Offices |
| `rste_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `lste_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `ofad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ofcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ubbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ubcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ubad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ubop_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `dler_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `dlad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `dlcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `bfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `bfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `cwte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm020 Warranty Templates |
| `swte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm020 Warranty Templates |
| `gwte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm500 Generic Warranties |
| `prio_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs070 Priorities |
| `cusc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsspc030 Usage Classes |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `ract_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp001 Availability Types |
| `cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs060 Manufacturers |
| `pste_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `clst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsbsc100 Installation Group |
| `titm_tser_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tscfg200 Serialized Items |
| `titm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm200 Items - Service |
| `ladr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ccnt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `csar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm105 Service Areas |
| `czon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm120 Distance Zones |
| `mdpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm100 Service Offices |
| `emno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm140 Service Employees |
| `prfb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm140 Service Employees |
| `prop_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm140 Service Employees |
| `sbct_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom121 Ship-from Business Partners |
| `shad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `tery_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm107 Territories |
| `pmsc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsspc130 Preventive Maintenance Scenarios |
| `rown_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm100 Service Offices |
| `rowc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

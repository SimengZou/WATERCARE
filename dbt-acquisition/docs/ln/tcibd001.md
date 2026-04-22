# tcibd001

LN tcibd001 Items table, Generated 2026-04-10T19:41:07Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcibd001` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `item` |
| **Column count** | 172 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `item` | `string` | `varchar` | `PK` | `not_null` | (required) Item. Max length: 47 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 60 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key I - base language. Max length: 16 |
| `seab__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key II - base language. Max length: 16 |
| `exin` | `string` | `varchar` |  |  | Extra Information. Max length: 8 |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modification Date |
| `cood` | `string` | `varchar` |  |  | Technical Coordinator. Max length: 9 |
| `kitm` | `integer` | `int` |  |  | Item Type. Allowed values: 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 35, 40, 50, 60, 70, 80, 90, 100, 110 |
| `kitm_kw` | `string` | `varchar` |  |  | Item Type (keyword). Allowed values: tckitm.purchase, tckitm.manufacture, tckitm.generic.obs, tckitm.cost.obs, tckitm.service.obs, tckitm.subcontract.obs, tckitm.list.obs, tckitm.tool.obs, tckitm.equipment.obs, tckitm.engineering.obs, tckitm.product, tckitm.rental.prod, tckitm.tool, tckitm.equipment, tckitm.subcontracting, tckitm.cost, tckitm.service, tckitm.generic, tckitm.engineering, tckitm.list |
| `citg` | `string` | `varchar` |  |  | Item Group. Max length: 6 |
| `itmt` | `integer` | `int` |  |  | List Type. Allowed values: 10, 20, 30, 40, 50 |
| `itmt_kw` | `string` | `varchar` |  |  | List Type (keyword). Allowed values: tcitmt.not.applicable, tcitmt.tool, tcitmt.kit, tcitmt.option, tcitmt.menu |
| `dsct` | `integer` | `int` |  |  | Dimension Controlled. Allowed values: 1, 2 |
| `dsct_kw` | `string` | `varchar` |  |  | Dimension Controlled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `osys` | `integer` | `int` |  |  | Order System. Allowed values: 1, 2, 4, 9 |
| `osys_kw` | `string` | `varchar` |  |  | Order System (keyword). Allowed values: tcosys.sic, tcosys.mps, tcosys.fas, tcosys.mnl |
| `cuni` | `string` | `varchar` |  |  | Inventory Unit. Max length: 3 |
| `rtun` | `string` | `varchar` |  |  | Rental Unit. Max length: 3 |
| `uset` | `string` | `varchar` |  |  | Unit Set. Max length: 6 |
| `ltct` | `integer` | `int` |  |  | Lot Controlled. Allowed values: 1, 2 |
| `ltct_kw` | `string` | `varchar` |  |  | Lot Controlled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `seri` | `integer` | `int` |  |  | Serialized. Allowed values: 1, 2 |
| `seri_kw` | `string` | `varchar` |  |  | Serialized (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cnfg` | `integer` | `int` |  |  | Configurable. Allowed values: 1, 2 |
| `cnfg_kw` | `string` | `varchar` |  |  | Configurable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `repl` | `integer` | `int` |  |  | Type of Replacement. Allowed values: 5, 10, 15, 20 |
| `repl_kw` | `string` | `varchar` |  |  | Type of Replacement (keyword). Allowed values: tcibd.repl.not.appl, tcibd.repl.repl, tcibd.repl.subst, tcibd.repl.repl.subst |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cpva` | `integer` | `int` |  |  | Product Variant |
| `dfit` | `string` | `varchar` |  |  | Derived-from Item. Max length: 47 |
| `stoi` | `integer` | `int` |  |  | Standard To Order. Allowed values: 1, 2 |
| `stoi_kw` | `string` | `varchar` |  |  | Standard To Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `opts` | `integer` | `int` |  |  | Option Set |
| `cust` | `integer` | `int` |  |  | Customized. Allowed values: 0, 1, 2 |
| `cust_kw` | `string` | `varchar` |  |  | Customized (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `opol` | `integer` | `int` |  |  | Customizable. Allowed values: 1, 2 |
| `opol_kw` | `string` | `varchar` |  |  | Customizable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wpcs` | `integer` | `int` |  |  | With PCS. Allowed values: 1, 2 |
| `wpcs_kw` | `string` | `varchar` |  |  | With PCS (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccfg` | `string` | `varchar` |  |  | Configuration Group. Max length: 6 |
| `unef` | `integer` | `int` |  |  | Unit Effective End Item. Allowed values: 1, 2 |
| `unef_kw` | `string` | `varchar` |  |  | Unit Effective End Item (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ichg` | `integer` | `int` |  |  | Effectivity Units are Interchangeable. Allowed values: 1, 2 |
| `ichg_kw` | `string` | `varchar` |  |  | Effectivity Units are Interchangeable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `eitm` | `integer` | `int` |  |  | Revision Controlled. Allowed values: 1, 2 |
| `eitm_kw` | `string` | `varchar` |  |  | Revision Controlled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uefs` | `integer` | `int` |  |  | Unit Effective Supply. Allowed values: 1, 2 |
| `uefs_kw` | `string` | `varchar` |  |  | Unit Effective Supply (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `umer` | `integer` | `int` |  |  | Update Method for E-Item Relation. Allowed values: 1, 2, 3, 4 |
| `umer_kw` | `string` | `varchar` |  |  | Update Method for E-Item Relation (keyword). Allowed values: tcibd.umer.yes, tcibd.umer.freeze, tcibd.umer.break, tcibd.umer.not.appl |
| `chma` | `integer` | `int` |  |  | Change Management Allowed. Allowed values: 1, 2 |
| `chma_kw` | `string` | `varchar` |  |  | Change Management Allowed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `efco` | `string` | `varchar` |  |  | Change Order. Max length: 8 |
| `indt` | `timestamp` | `timestamp_ntz` |  |  | Effective Date |
| `edco` | `integer` | `int` |  |  | Effectivity Dates by Change Order. Allowed values: 1, 2 |
| `edco_kw` | `string` | `varchar` |  |  | Effectivity Dates by Change Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mcoa` | `integer` | `int` |  |  | Multiple Change Orders Allowed. Allowed values: 1, 2 |
| `mcoa_kw` | `string` | `varchar` |  |  | Multiple Change Orders Allowed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sayn` | `integer` | `int` |  |  | Subassembly. Allowed values: 1, 2 |
| `sayn_kw` | `string` | `varchar` |  |  | Subassembly (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cont` | `integer` | `int` |  |  | Container Item. Allowed values: 1, 2 |
| `cont_kw` | `string` | `varchar` |  |  | Container Item (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cntr` | `string` | `varchar` |  |  | Container. Max length: 3 |
| `cpcp` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `coeu` | `integer` | `int` |  |  | Standard Costs at Level. Allowed values: 5, 10, 15 |
| `coeu_kw` | `string` | `varchar` |  |  | Standard Costs at Level (keyword). Allowed values: tccoeu.company, tccoeu.ent.unit, tccoeu.not.appl |
| `ppeg` | `integer` | `int` |  |  | Mandatory Project Peg. Allowed values: 1, 2 |
| `ppeg_kw` | `string` | `varchar` |  |  | Mandatory Project Peg (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ippg` | `integer` | `int` |  |  | Inherit Project Peg. Allowed values: 1, 2 |
| `ippg_kw` | `string` | `varchar` |  |  | Inherit Project Peg (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ppss` | `integer` | `int` |  |  | Project Pegged Safety Stock Allowed. Allowed values: 1, 2 |
| `ppss_kw` | `string` | `varchar` |  |  | Project Pegged Safety Stock Allowed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `elcm` | `integer` | `int` |  |  | Restricted by Components. Allowed values: 1, 2 |
| `elcm_kw` | `string` | `varchar` |  |  | Restricted by Components (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `elrq` | `integer` | `int` |  |  | Restrict Commingling and Transfers. Allowed values: 1, 2 |
| `elrq_kw` | `string` | `varchar` |  |  | Restrict Commingling and Transfers (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dpeg` | `integer` | `int` |  |  | Demand Pegged. Allowed values: 1, 2 |
| `dpeg_kw` | `string` | `varchar` |  |  | Demand Pegged (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dptp` | `integer` | `int` |  |  | Demand Pegging Type. Allowed values: 5, 7, 10, 15, 20, 90 |
| `dptp_kw` | `string` | `varchar` |  |  | Demand Pegging Type (keyword). Allowed values: tcpgtp.bp.based, tcpgtp.bploc.based, tcpgtp.order.based, tcpgtp.cust.ref.based, tcpgtp.int.ref.based, tcpgtp.not.appl |
| `dpuu` | `integer` | `int` |  |  | Use Unallocated Inventory. Allowed values: 1, 2 |
| `dpuu_kw` | `string` | `varchar` |  |  | Use Unallocated Inventory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sgtc` | `integer` | `int` |  |  | Subject to Trade Compliance. Allowed values: 1, 2 |
| `sgtc_kw` | `string` | `varchar` |  |  | Subject to Trade Compliance (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lctr` | `integer` | `int` |  |  | License Tracking. Allowed values: 1, 2 |
| `lctr_kw` | `string` | `varchar` |  |  | License Tracking (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cltr` | `integer` | `int` |  |  | Contains License Tracking. Allowed values: 1, 2 |
| `cltr_kw` | `string` | `varchar` |  |  | Contains License Tracking (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `srce` | `integer` | `int` |  |  | Default Supply Source. Allowed values: 10, 20, 25, 30, 40, 50, 60 |
| `srce_kw` | `string` | `varchar` |  |  | Default Supply Source (keyword). Allowed values: tcsrce.not.appl, tcsrce.shopfloor, tcsrce.repetitive, tcsrce.assembly, tcsrce.purchase, tcsrce.subcontract, tcsrce.distribution |
| `efpr` | `integer` | `int` |  |  | Date-Effective Item Sources. Allowed values: 1, 2 |
| `efpr_kw` | `string` | `varchar` |  |  | Date-Effective Item Sources (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dscb__en_us` | `string` | `varchar` |  | `not_null` | (required) Material - base language. Max length: 30 |
| `dscc__en_us` | `string` | `varchar` |  | `not_null` | (required) Size - base language. Max length: 30 |
| `dscd__en_us` | `string` | `varchar` |  | `not_null` | (required) Standard - base language. Max length: 30 |
| `wght` | `float` | `float` |  |  | Weight |
| `cwun` | `string` | `varchar` |  |  | Weight Unit. Max length: 3 |
| `ctyp` | `string` | `varchar` |  |  | Product Type. Max length: 3 |
| `cpcl` | `string` | `varchar` |  |  | Product Class. Max length: 6 |
| `cpln` | `string` | `varchar` |  |  | Product Line. Max length: 6 |
| `cmnf` | `string` | `varchar` |  |  | Manufacturer. Max length: 6 |
| `csel` | `string` | `varchar` |  |  | Selection Code. Max length: 3 |
| `csig` | `string` | `varchar` |  |  | Item Signal. Max length: 3 |
| `rdpt` | `string` | `varchar` |  |  | Responsible Department. Max length: 6 |
| `ctyo` | `string` | `varchar` |  |  | Country of Origin. Max length: 3 |
| `envc` | `integer` | `int` |  |  | Environmental Compliance. Allowed values: 5, 10, 15, 20, 25, 30, 35, 40 |
| `envc_kw` | `string` | `varchar` |  |  | Environmental Compliance (keyword). Allowed values: tcenvc.not.appl, tcenvc.not.valid, tcenvc.compliant, tcenvc.compl.except, tcenvc.not.compl, tcenvc.cond.fail, tcenvc.part.fail, tcenvc.not.rele |
| `cean` | `string` | `varchar` |  |  | EAN Code. Max length: 14 |
| `ccde` | `string` | `varchar` |  |  | Harmonized System Code. Max length: 25 |
| `icsi` | `integer` | `int` |  |  | Critical Safety Item. Allowed values: 1, 2 |
| `icsi_kw` | `string` | `varchar` |  |  | Critical Safety Item (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fstk` | `integer` | `int` |  |  | Floor Stock. Allowed values: 1, 2 |
| `fstk_kw` | `string` | `varchar` |  |  | Floor Stock (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `psiu` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `psiu_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `styp` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2, 3 |
| `styp_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcstyp.not.applicable, tcstyp.pull, tcstyp.push |
| `subc` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `subc_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oktm` | `integer` | `int` |  |  | Obsolete |
| `dpcr` | `integer` | `int` |  |  | Obsolete. Allowed values: 10, 20, 100 |
| `dpcr_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcpgcr.always, tcpgcr.sls.contract, tcpgcr.not.apl |
| `rvdi` | `integer` | `int` |  |  | Revision Data in IBD. Allowed values: 1, 2 |
| `rvdi_kw` | `string` | `varchar` |  |  | Revision Data in IBD (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rnum` | `integer` | `int` |  |  | Rental Number. Allowed values: 10, 20, 100 |
| `rnum_kw` | `string` | `varchar` |  |  | Rental Number (keyword). Allowed values: tcitm.rnum.prod, tcitm.rnum.rent, tcitm.rnum.not.appl |
| `mdtp` | `integer` | `int` |  |  | Module Type. Allowed values: 10, 20, 100 |
| `mdtp_kw` | `string` | `varchar` |  |  | Module Type (keyword). Allowed values: tcibd.mdtp.eng.mod, tcibd.mdtp.mou.list, tcibd.mdtp.not.appl |
| `iitv` | `float` | `float` |  |  | Indicative Item Value in Reference Currency |
| `iivc` | `string` | `varchar` |  |  | Indicative Item Value Currency. Max length: 3 |
| `iivu` | `string` | `varchar` |  |  | Indicative Item Value Unit. Max length: 3 |
| `ivmd` | `timestamp` | `timestamp_ntz` |  |  | Indicative Item Value Modification Date |
| `ivbd` | `integer` | `int` |  |  | Indicative Item Value Based on Default. Allowed values: 1, 2 |
| `ivbd_kw` | `string` | `varchar` |  |  | Indicative Item Value Based on Default (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Item Text |
| `cood_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `citg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs023 Item Groups |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `rtun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `uset_cwun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs012 Units by Unit Set |
| `uset_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs006 Unit Sets |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `dfit_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `ccfg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs150 Configuration Group |
| `cntr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cpcp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `cwun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ctyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs015 Product Types |
| `cpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs062 Product Classes |
| `cpln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs061 Product Lines |
| `cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs060 Manufacturers |
| `csel_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs022 Selection Codes |
| `csig_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs018 Item Signals |
| `rdpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `ctyo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `ccde_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs028 Harmonized System Codes |
| `iivc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `iivu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `cpva_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Product Variant |
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

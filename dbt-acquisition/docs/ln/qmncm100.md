# qmncm100

LN qmncm100 Non-Conformance Report (NCR) table, Generated 2022-06-15T01:21:02Z from package combination ce01055

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_qmncm100` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ncmr` |
| **Column count** | 204 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ncmr` | `string` | `varchar` | `PK` | `not_null` | (required) Non-Conformance Report. Max length: 9 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `serl` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `huid` | `string` | `varchar` |  |  | Handling Unit. Max length: 25 |
| `revi` | `string` | `varchar` |  |  | E-Item Revision. Max length: 6 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `nqua` | `float` | `float` |  |  | Non-Conforming Material Quantity |
| `nuom` | `string` | `varchar` |  |  | Unit of Measure. Max length: 3 |
| `mitm` | `string` | `varchar` |  |  | Parent Item. Max length: 47 |
| `bpid` | `string` | `varchar` |  |  | Business Partner. Max length: 9 |
| `orgn` | `integer` | `int` |  |  | Order Origin. Allowed values: 1, 2, 3, 4, 50, 51, 53, 55, 56, 61, 62, 63, 64, 65, 71, 72, 75, 76, 78, 80, 81, 82, 90, 95, 96, 97, 100 |
| `orgn_kw` | `string` | `varchar` |  |  | Order Origin (keyword). Allowed values: qmncm.orgn.sales, qmncm.orgn.sales.sched, qmncm.orgn.sales.man, qmncm.orgn.adjustment, qmncm.orgn.production, qmncm.orgn.production.man, qmncm.orgn.product.sched, qmncm.orgn.product.asc, qmncm.orgn.product.asc.man, qmncm.orgn.service, qmncm.orgn.maint.work, qmncm.orgn.batch.repair, qmncm.orgn.maint.sales, qmncm.orgn.call, qmncm.orgn.transfer, qmncm.orgn.transfer.man, qmncm.orgn.project, qmncm.orgn.project.man, qmncm.orgn.proj.contract, qmncm.orgn.purchase, qmncm.orgn.purchase.sched, qmncm.orgn.purchase.man, qmncm.orgn.enterprise.plan, qmncm.orgn.stor.insp, qmncm.orgn.warehouse.inv, qmncm.orgn.inv.insp, qmncm.orgn.not.appl |
| `orno` | `string` | `varchar` |  |  | Origin Order. Max length: 9 |
| `pono` | `integer` | `int` |  |  | Order Position |
| `seqn` | `integer` | `int` |  |  | Sequence |
| `opno` | `integer` | `int` |  |  | Operation |
| `wstt` | `string` | `varchar` |  |  | Work Station. Max length: 6 |
| `apno` | `string` | `varchar` |  |  | Warehouse Inspection. Max length: 9 |
| `apsq` | `integer` | `int` |  |  | Warehouse Inspection Sequence |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `loca` | `string` | `varchar` |  |  | Location. Max length: 10 |
| `cwoc` | `string` | `varchar` |  |  | Department. Max length: 6 |
| `nctp` | `string` | `varchar` |  |  | Non-Conforming Type. Max length: 9 |
| `slvl` | `string` | `varchar` |  |  | Material Non-Conformance Severity. Max length: 9 |
| `mrbc` | `string` | `varchar` |  |  | Review Board. Max length: 9 |
| `mrbr` | `integer` | `int` |  |  | MRB Responsible for Disposition. Allowed values: 1, 2 |
| `mrbr_kw` | `string` | `varchar` |  |  | MRB Responsible for Disposition (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rptr` | `string` | `varchar` |  |  | Reported By. Max length: 16 |
| `rpdt` | `timestamp` | `timestamp_ntz` |  |  | Reported Date |
| `capc` | `string` | `varchar` |  |  | Corrective Action Plan. Max length: 9 |
| `ownr` | `string` | `varchar` |  |  | Owner. Max length: 9 |
| `prvd` | `timestamp` | `timestamp_ntz` |  |  | Planned Review Date |
| `disp` | `integer` | `int` |  |  | Disposition. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 20 |
| `disp_kw` | `string` | `varchar` |  |  | Disposition (keyword). Allowed values: qmncm.disp.rework.curr, qmncm.disp.rework.new, qmncm.disp.reclassify, qmncm.disp.return.vendor, qmncm.disp.scrap, qmncm.disp.use.as.is, qmncm.disp.repair, qmncm.disp.no.fault, qmncm.disp.undefined, qmncm.disp.not.appl |
| `ncmc` | `string` | `varchar` |  |  | Non-Conforming Material Cause. Max length: 9 |
| `ncrc` | `string` | `varchar` |  |  | Non-Conforming Material Responsible. Max length: 9 |
| `rsbp` | `string` | `varchar` |  |  | Responsible Business Partner. Max length: 9 |
| `wvbp` | `string` | `varchar` |  |  | Sold-to Business Partner (Waiver). Max length: 9 |
| `wvrd` | `integer` | `int` |  |  | Waiver Required. Allowed values: 1, 2 |
| `wvrd_kw` | `string` | `varchar` |  |  | Waiver Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wvmd` | `integer` | `int` |  |  | Waiver Mandatory. Allowed values: 1, 2 |
| `wvmd_kw` | `string` | `varchar` |  |  | Waiver Mandatory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wvdr` | `timestamp` | `timestamp_ntz` |  |  | Waiver Receipt Date |
| `stat` | `integer` | `int` |  |  | Non-Conforming Material Report Status. Allowed values: 1, 2, 3, 4, 5, 6 |
| `stat_kw` | `string` | `varchar` |  |  | Non-Conforming Material Report Status (keyword). Allowed values: qmncm.stat.open, qmncm.stat.submitted, qmncm.stat.assigned, qmncm.stat.dispositioned, qmncm.stat.cancelled, qmncm.stat.closed |
| `dorg` | `integer` | `int` |  |  | Disposition Order Origin. Allowed values: 1, 2, 3, 4, 50, 51, 53, 55, 56, 61, 62, 63, 64, 65, 71, 72, 75, 76, 78, 80, 81, 82, 90, 95, 96, 97, 100 |
| `dorg_kw` | `string` | `varchar` |  |  | Disposition Order Origin (keyword). Allowed values: qmncm.orgn.sales, qmncm.orgn.sales.sched, qmncm.orgn.sales.man, qmncm.orgn.adjustment, qmncm.orgn.production, qmncm.orgn.production.man, qmncm.orgn.product.sched, qmncm.orgn.product.asc, qmncm.orgn.product.asc.man, qmncm.orgn.service, qmncm.orgn.maint.work, qmncm.orgn.batch.repair, qmncm.orgn.maint.sales, qmncm.orgn.call, qmncm.orgn.transfer, qmncm.orgn.transfer.man, qmncm.orgn.project, qmncm.orgn.project.man, qmncm.orgn.proj.contract, qmncm.orgn.purchase, qmncm.orgn.purchase.sched, qmncm.orgn.purchase.man, qmncm.orgn.enterprise.plan, qmncm.orgn.stor.insp, qmncm.orgn.warehouse.inv, qmncm.orgn.inv.insp, qmncm.orgn.not.appl |
| `dior` | `string` | `varchar` |  |  | Disposition Order. Max length: 9 |
| `diln` | `integer` | `int` |  |  | Disposition Order Position |
| `didt` | `timestamp` | `timestamp_ntz` |  |  | Disposition Date |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `resn` | `string` | `varchar` |  |  | Reject Reason. Max length: 6 |
| `cldt` | `timestamp` | `timestamp_ntz` |  |  | Closing Date |
| `suby` | `string` | `varchar` |  |  | Last Submitted By. Max length: 16 |
| `asby` | `string` | `varchar` |  |  | Last Assigned By. Max length: 16 |
| `diby` | `string` | `varchar` |  |  | Last Dispositioned By. Max length: 16 |
| `reby` | `string` | `varchar` |  |  | Last Reset By. Max length: 16 |
| `spby` | `string` | `varchar` |  |  | Last Split By. Max length: 16 |
| `clby` | `string` | `varchar` |  |  | Closed By. Max length: 16 |
| `caby` | `string` | `varchar` |  |  | Cancelled By. Max length: 16 |
| `sudt` | `timestamp` | `timestamp_ntz` |  |  | Submitted Date |
| `asdt` | `timestamp` | `timestamp_ntz` |  |  | Assigned Date |
| `redt` | `timestamp` | `timestamp_ntz` |  |  | Reset Date |
| `spdt` | `timestamp` | `timestamp_ntz` |  |  | Split Date |
| `cadt` | `timestamp` | `timestamp_ntz` |  |  | Cancelled Date |
| `pncm` | `string` | `varchar` |  |  | Parent NCMR. Max length: 9 |
| `citw` | `string` | `varchar` |  |  | Customer Item Code System. Max length: 3 |
| `criw__en_us` | `string` | `varchar` |  | `not_null` | (required) Cross Reference Item - base language. Max length: 35 |
| `docn` | `string` | `varchar` |  |  | Document Number. Max length: 15 |
| `drdt` | `timestamp` | `timestamp_ntz` |  |  | Document Received Date |
| `citd` | `string` | `varchar` |  |  | Supplier Item Code System. Max length: 3 |
| `crid__en_us` | `string` | `varchar` |  | `not_null` | (required) Cross Reference Item - base language. Max length: 35 |
| `styp` | `integer` | `int` |  |  | Line Type. Allowed values: 1, 2, 3, 4, 5 |
| `styp_kw` | `string` | `varchar` |  |  | Line Type (keyword). Allowed values: qmptc.styp.material.line, qmptc.styp.incoming.sub, qmptc.styp.outgoing.sub, qmptc.styp.activity.line, qmptc.styp.not.appl |
| `dept` | `string` | `varchar` |  |  | Department(Internal). Max length: 6 |
| `bpty` | `integer` | `int` |  |  | Business Partner Type. Allowed values: 0, 10, 20, 30 |
| `bpty_kw` | `string` | `varchar` |  |  | Business Partner Type (keyword). Allowed values: , qmncm.bpty.supplier, qmncm.bpty.customer, qmncm.bpty.not.appl |
| `ccnt` | `string` | `varchar` |  |  | Contact. Max length: 9 |
| `rtbp` | `string` | `varchar` |  |  | Reporting Business Partner. Max length: 9 |
| `ncmp__en_us` | `string` | `varchar` |  | `not_null` | (required) Company - base language. Max length: 30 |
| `telp` | `string` | `varchar` |  |  | Phone. Max length: 32 |
| `mail__en_us` | `string` | `varchar` |  | `not_null` | (required) Mail - base language. Max length: 100 |
| `svty` | `integer` | `int` |  |  | Severity. Allowed values: 1, 2, 3, 4 |
| `svty_kw` | `string` | `varchar` |  |  | Severity (keyword). Allowed values: qmncm.svty.critical, qmncm.svty.major, qmncm.svty.minor, qmncm.svty.not.appl |
| `owbp` | `string` | `varchar` |  |  | Buy From Business Partner (Outgoing). Max length: 9 |
| `owrd` | `integer` | `int` |  |  | Waiver (outgoing) Required. Allowed values: 0, 1, 2 |
| `owrd_kw` | `string` | `varchar` |  |  | Waiver (outgoing) Required (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `drst` | `timestamp` | `timestamp_ntz` |  |  | Document Sent Date |
| `odoc` | `string` | `varchar` |  |  | Document Number. Max length: 15 |
| `ncca` | `string` | `varchar` |  |  | Non Conformance Cause. Max length: 9 |
| `ncrr` | `string` | `varchar` |  |  | Non Conformance Responsible. Max length: 9 |
| `ndis` | `string` | `varchar` |  |  | Non Conformance Disposition Code. Max length: 9 |
| `ncap` | `string` | `varchar` |  |  | Non Conformance Corrective Action Plan. Max length: 9 |
| `mdis` | `string` | `varchar` |  |  | Material Disposition. Max length: 9 |
| `disr` | `string` | `varchar` |  |  | Reason. Max length: 6 |
| `ditm` | `string` | `varchar` |  |  | New Item. Max length: 47 |
| `dist` | `integer` | `int` |  |  | Obsolete |
| `mskl` | `string` | `varchar` |  |  | Skill Required - Material. Max length: 9 |
| `memp` | `string` | `varchar` |  |  | Assigned Employee - Material. Max length: 9 |
| `mtpe` | `float` | `float` |  |  | Time Estimate - Material |
| `mtun` | `string` | `varchar` |  |  | Time Unit - Material. Max length: 3 |
| `msdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Start Date - Material |
| `mcdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Completion Date - Material |
| `mast` | `timestamp` | `timestamp_ntz` |  |  | Actual Start Date - Material |
| `mact` | `timestamp` | `timestamp_ntz` |  |  | Actual Completion Date - Material |
| `nskl` | `string` | `varchar` |  |  | Skill Required - Non Material. Max length: 9 |
| `nemp` | `string` | `varchar` |  |  | Assigned Employee - Non Material. Max length: 9 |
| `ntpe` | `float` | `float` |  |  | Time Estimate - Non Material |
| `ntun` | `string` | `varchar` |  |  | Time Unit - Non Material. Max length: 3 |
| `nsdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Start Date - Non Material |
| `ncdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Completion Date - Non Material |
| `nast` | `timestamp` | `timestamp_ntz` |  |  | Actual Start Date - Non-Material |
| `nact` | `timestamp` | `timestamp_ntz` |  |  | Actual Completion Date - Non-Material |
| `prmd` | `string` | `varchar` |  |  | Production Model. Max length: 9 |
| `pmrv` | `string` | `varchar` |  |  | Production Model Revision. Max length: 6 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `qtds` | `integer` | `int` |  |  | Quarantine Disposition. Allowed values: 1, 2 |
| `qtds_kw` | `string` | `varchar` |  |  | Quarantine Disposition (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ncnm` | `integer` | `int` |  |  | Non-Material Only. Allowed values: 1, 2 |
| `ncnm_kw` | `string` | `varchar` |  |  | Non-Material Only (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bpcm` | `integer` | `int` |  |  | Business Partner Communication Needed. Allowed values: 1, 2 |
| `bpcm_kw` | `string` | `varchar` |  |  | Business Partner Communication Needed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `scbp` | `string` | `varchar` |  |  | Business Partner to be Communicated. Max length: 9 |
| `bprr` | `integer` | `int` |  |  | Business Partner Response Required. Allowed values: 1, 2 |
| `bprr_kw` | `string` | `varchar` |  |  | Business Partner Response Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `plds` | `string` | `varchar` |  |  | Planned Disposition. Max length: 9 |
| `bpsd` | `string` | `varchar` |  |  | Business Partner Suggested Disposition. Max length: 9 |
| `erdt` | `timestamp` | `timestamp_ntz` |  |  | Expected Response Date |
| `rsst` | `integer` | `int` |  |  | Response Status. Allowed values: 10, 20, 30, 40, 100 |
| `rsst_kw` | `string` | `varchar` |  |  | Response Status (keyword). Allowed values: qmncm.rsst.awa, qmncm.rsst.rec, qmncm.rsst.acc, qmncm.rsst.rej, qmncm.rsst.not.appl |
| `dstx` | `integer` | `int` |  |  | Disposition Text |
| `nctx` | `integer` | `int` |  |  | Description of Non-Conformance |
| `edtx` | `integer` | `int` |  |  | Expected Disposition Text |
| `ndtx` | `integer` | `int` |  |  | Non Conformance Additional Details |
| `obtx` | `integer` | `int` |  |  | Observation |
| `attx` | `integer` | `int` |  |  | Attribution |
| `evtx` | `integer` | `int` |  |  | Objective Evidence |
| `intx` | `integer` | `int` |  |  | Internal Text |
| `bctx` | `integer` | `int` |  |  | Business Partner Communication Text |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `nuom_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `mitm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `nctp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmncm001 Non-Conforming Material Types |
| `slvl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmncm002 Severity |
| `ownr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `ncmc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmncm004 Non-Conforming Cause |
| `ncrc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmncm005 Responsibility |
| `rsbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `wvbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `resn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `citw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd006 Item Code Systems |
| `citd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd006 Item Code Systems |
| `dept_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `ccnt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `rtbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `owbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ncca_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmncm004 Non-Conforming Cause |
| `ncrr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmncm005 Responsibility |
| `ndis_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmncm006 Non Conformance Dispositions |
| `mdis_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmncm007 Material Dispositions |
| `disr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `ditm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd001 Items |
| `mskl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl010 Skills |
| `memp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `mtun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `nskl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcppl010 Skills |
| `nemp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `ntun_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `scbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `plds_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmncm007 Material Dispositions |
| `bpsd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table qmncm007 Material Dispositions |
| `dstx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `nctx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `edtx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `ndtx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `obtx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `attx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `evtx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `intx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `bctx_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `nqua_buar` | `float` | `float` |  |  | CC: Non Confirming Material Quantity in Base Unit Area |
| `nqua_buln` | `float` | `float` |  |  | CC: Non Confirming Material Quantity in Base Unit Length |
| `nqua_bupc` | `float` | `float` |  |  | CC: Non Confirming Material Quantity in Base Unit Piece |
| `nqua_butm` | `float` | `float` |  |  | CC: Non Confirming Material Quantity in Base Unit Time |
| `nqua_buvl` | `float` | `float` |  |  | CC: Non Confirming Material Quantity in Base Unit Volume |
| `nqua_buwg` | `float` | `float` |  |  | CC: Non Confirming Material Quantity in Base Unit Weight |
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

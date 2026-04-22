# tdpur401

LN tdpur401 Purchase Order Lines table, Generated 2026-04-10T19:41:22Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur401` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orno`, `pono`, `sqnb` |
| **Column count** | 469 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Purchase Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `sqnb` | `integer` | `int` | `PK` | `not_null` | (required) Sequence |
| `corg` | `integer` | `int` |  |  | Origin. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 27, 29, 30, 99 |
| `corg_kw` | `string` | `varchar` |  |  | Origin (keyword). Allowed values: tdpur.corg.mrp, tdpur.corg.wcs, tdpur.corg.inv, tdpur.corg.contracts, tdpur.corg.inquiries, tdpur.corg.eop, tdpur.corg.manual, tdpur.corg.sfc, tdpur.corg.project, tdpur.corg.sls, tdpur.corg.sls.schedule, tdpur.corg.sma, tdpur.corg.pmg, tdpur.corg.requisition, tdpur.corg.copy.actual, tdpur.corg.copy.history, tdpur.corg.asc, tdpur.corg.comm, tdpur.corg.schedules, tdpur.corg.extern, tdpur.corg.wh.receipt, tdpur.corg.payment, tdpur.corg.subc.pur.order, tdpur.corg.subc.pur.sched, tdpur.corg.serv.cust.claim, tdpur.corg.routing, tdpur.corg.lett.of.credit, tdpur.corg.price.calc, tdpur.corg.not.appl |
| `oltp` | `integer` | `int` |  |  | Order Line Type. Allowed values: 1, 2, 3, 4 |
| `oltp_kw` | `string` | `varchar` |  |  | Order Line Type (keyword). Allowed values: tdgen.oltp.total, tdgen.oltp.detail, tdgen.oltp.backorder, tdgen.oltp.orderline |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `sfad` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `sfcn` | `string` | `varchar` |  |  | Ship-from Contact. Max length: 9 |
| `sfsi` | `string` | `varchar` |  |  | Ship-from Site. Max length: 9 |
| `sfwh` | `string` | `varchar` |  |  | Ship-from Warehouse. Max length: 6 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `atse` | `string` | `varchar` |  |  | Attribute Set. Max length: 35 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `sdsc` | `integer` | `int` |  |  | Standard Description. Allowed values: 1, 2 |
| `sdsc_kw` | `string` | `varchar` |  |  | Standard Description (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crrf` | `integer` | `int` |  |  | Item Cross Reference. Allowed values: 1, 2, 3 |
| `crrf_kw` | `string` | `varchar` |  |  | Item Cross Reference (keyword). Allowed values: tccrrf.ics, tccrrf.mpn, tccrrf.na |
| `citt` | `string` | `varchar` |  |  | Code Item Type. Max length: 3 |
| `crit__en_us` | `string` | `varchar` |  | `not_null` | (required) Cross Reference Item - base language. Max length: 35 |
| `mpnr` | `string` | `varchar` |  |  | Preferred Manufacturer Part Number. Max length: 35 |
| `subc` | `integer` | `int` |  |  | Subcontracted. Allowed values: 1, 2 |
| `subc_kw` | `string` | `varchar` |  |  | Subcontracted (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `subs` | `string` | `varchar` |  |  | Subcontractor Site. Max length: 9 |
| `mpsn` | `string` | `varchar` |  |  | Manufacturer Part Number Set. Max length: 22 |
| `cmnf` | `string` | `varchar` |  |  | Manufacturer. Max length: 6 |
| `mitm__en_us` | `string` | `varchar` |  | `not_null` | (required) Manufacturer Item - base language. Max length: 35 |
| `revi` | `string` | `varchar` |  |  | Engineering Item Revision. Max length: 6 |
| `acqm` | `integer` | `int` |  |  | Acquisition Method. Allowed values: 5, 10, 99 |
| `acqm_kw` | `string` | `varchar` |  |  | Acquisition Method (keyword). Allowed values: tcacqm.buying, tcacqm.rental, tcacqm.not.appl |
| `btsp` | `integer` | `int` |  |  | Inventory Posting Status (Purchase). Allowed values: 1, 2 |
| `btsp_kw` | `string` | `varchar` |  |  | Inventory Posting Status (Purchase) (keyword). Allowed values: tcinup.by.main.item, tcinup.by.component |
| `qual` | `integer` | `int` |  |  | Inspection. Allowed values: 1, 2 |
| `qual_kw` | `string` | `varchar` |  |  | Inspection (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `qpor` | `float` | `float` |  |  | Ordered Quantity in Piece Unit |
| `cupq` | `string` | `varchar` |  |  | Order Piece Unit. Max length: 3 |
| `cvpq` | `float` | `float` |  |  | Order Piece Unit Conversion Factor |
| `qoor` | `float` | `float` |  |  | Ordered Quantity |
| `cuqp` | `string` | `varchar` |  |  | Purchase Unit. Max length: 3 |
| `cvqp` | `float` | `float` |  |  | Conversion Factor Purchase to Inventory Unit |
| `leng` | `float` | `float` |  |  | Length |
| `widt` | `float` | `float` |  |  | Width |
| `thic` | `float` | `float` |  |  | Height |
| `odat` | `timestamp` | `timestamp_ntz` |  |  | Order Date |
| `ddta` | `timestamp` | `timestamp_ntz` |  |  | Planned Receipt Date |
| `ddtb` | `timestamp` | `timestamp_ntz` |  |  | Current Planned Receipt Date |
| `ddtc` | `timestamp` | `timestamp_ntz` |  |  | Confirmed Receipt Date |
| `ddtd` | `timestamp` | `timestamp_ntz` |  |  | Changed Receipt Date |
| `ddte` | `timestamp` | `timestamp_ntz` |  |  | Actual Receipt Date |
| `ddtf` | `timestamp` | `timestamp_ntz` |  |  | Order Confirmation Date |
| `rdta` | `timestamp` | `timestamp_ntz` |  |  | Release Date |
| `pric` | `float` | `float` |  |  | Price |
| `prsg` | `string` | `varchar` |  |  | Price Stage. Max length: 3 |
| `porg` | `integer` | `int` |  |  | Price Origin. Allowed values: 2, 4, 6, 8, 10, 12, 13, 14, 16, 18, 20, 25, 30, 35, 40 |
| `porg_kw` | `string` | `varchar` |  |  | Price Origin (keyword). Allowed values: tdgen.porg.not.applicable, tdgen.porg.manual, tdgen.porg.contract, tdgen.porg.variant, tdgen.porg.item.pur, tdgen.porg.item.sls, tdgen.porg.item.service, tdgen.porg.supp.pr.book, tdgen.porg.def.pr.book, tdgen.porg.price.structure, tdgen.porg.extern, tdgen.porg.consumption, tdgen.porg.generic.pr.list, tdgen.porg.project.budget, tdgen.porg.projectcost.obj |
| `pmde` | `string` | `varchar` |  |  | Price Matrix. Max length: 9 |
| `pmse` | `integer` | `int` |  |  | Price Matrix Sequence |
| `cupp` | `string` | `varchar` |  |  | Purchase Price Unit. Max length: 3 |
| `cvpp` | `float` | `float` |  |  | Conversion Factor Price to Inventory Unit |
| `disc_1` | `float` | `float` |  |  | Line Discount |
| `disc_2` | `float` | `float` |  |  | Line Discount |
| `disc_3` | `float` | `float` |  |  | Line Discount |
| `disc_4` | `float` | `float` |  |  | Line Discount |
| `disc_5` | `float` | `float` |  |  | Line Discount |
| `disc_6` | `float` | `float` |  |  | Line Discount |
| `disc_7` | `float` | `float` |  |  | Line Discount |
| `disc_8` | `float` | `float` |  |  | Line Discount |
| `disc_9` | `float` | `float` |  |  | Line Discount |
| `disc_10` | `float` | `float` |  |  | Line Discount |
| `disc_11` | `float` | `float` |  |  | Line Discount |
| `ldam_1` | `float` | `float` |  |  | Discount Amount |
| `ldam_2` | `float` | `float` |  |  | Discount Amount |
| `ldam_3` | `float` | `float` |  |  | Discount Amount |
| `ldam_4` | `float` | `float` |  |  | Discount Amount |
| `ldam_5` | `float` | `float` |  |  | Discount Amount |
| `ldam_6` | `float` | `float` |  |  | Discount Amount |
| `ldam_7` | `float` | `float` |  |  | Discount Amount |
| `ldam_8` | `float` | `float` |  |  | Discount Amount |
| `ldam_9` | `float` | `float` |  |  | Discount Amount |
| `ldam_10` | `float` | `float` |  |  | Discount Amount |
| `ldam_11` | `float` | `float` |  |  | Discount Amount |
| `dorg_1` | `integer` | `int` |  |  | Discount Origin. Allowed values: 2, 4, 6, 8, 10, 12 |
| `dorg_kw_1` | `string` | `varchar` |  |  | Discount Origin (keyword). Allowed values: tdgen.dorg.not.applicable, tdgen.dorg.manual, tdgen.dorg.contract, tdgen.dorg.disc.structure, tdgen.dorg.disc.pr.book, tdgen.dorg.extern |
| `dorg_2` | `integer` | `int` |  |  | Discount Origin. Allowed values: 2, 4, 6, 8, 10, 12 |
| `dorg_kw_2` | `string` | `varchar` |  |  | Discount Origin (keyword). Allowed values: tdgen.dorg.not.applicable, tdgen.dorg.manual, tdgen.dorg.contract, tdgen.dorg.disc.structure, tdgen.dorg.disc.pr.book, tdgen.dorg.extern |
| `dorg_3` | `integer` | `int` |  |  | Discount Origin. Allowed values: 2, 4, 6, 8, 10, 12 |
| `dorg_kw_3` | `string` | `varchar` |  |  | Discount Origin (keyword). Allowed values: tdgen.dorg.not.applicable, tdgen.dorg.manual, tdgen.dorg.contract, tdgen.dorg.disc.structure, tdgen.dorg.disc.pr.book, tdgen.dorg.extern |
| `dorg_4` | `integer` | `int` |  |  | Discount Origin. Allowed values: 2, 4, 6, 8, 10, 12 |
| `dorg_kw_4` | `string` | `varchar` |  |  | Discount Origin (keyword). Allowed values: tdgen.dorg.not.applicable, tdgen.dorg.manual, tdgen.dorg.contract, tdgen.dorg.disc.structure, tdgen.dorg.disc.pr.book, tdgen.dorg.extern |
| `dorg_5` | `integer` | `int` |  |  | Discount Origin. Allowed values: 2, 4, 6, 8, 10, 12 |
| `dorg_kw_5` | `string` | `varchar` |  |  | Discount Origin (keyword). Allowed values: tdgen.dorg.not.applicable, tdgen.dorg.manual, tdgen.dorg.contract, tdgen.dorg.disc.structure, tdgen.dorg.disc.pr.book, tdgen.dorg.extern |
| `dorg_6` | `integer` | `int` |  |  | Discount Origin. Allowed values: 2, 4, 6, 8, 10, 12 |
| `dorg_kw_6` | `string` | `varchar` |  |  | Discount Origin (keyword). Allowed values: tdgen.dorg.not.applicable, tdgen.dorg.manual, tdgen.dorg.contract, tdgen.dorg.disc.structure, tdgen.dorg.disc.pr.book, tdgen.dorg.extern |
| `dorg_7` | `integer` | `int` |  |  | Discount Origin. Allowed values: 2, 4, 6, 8, 10, 12 |
| `dorg_kw_7` | `string` | `varchar` |  |  | Discount Origin (keyword). Allowed values: tdgen.dorg.not.applicable, tdgen.dorg.manual, tdgen.dorg.contract, tdgen.dorg.disc.structure, tdgen.dorg.disc.pr.book, tdgen.dorg.extern |
| `dorg_8` | `integer` | `int` |  |  | Discount Origin. Allowed values: 2, 4, 6, 8, 10, 12 |
| `dorg_kw_8` | `string` | `varchar` |  |  | Discount Origin (keyword). Allowed values: tdgen.dorg.not.applicable, tdgen.dorg.manual, tdgen.dorg.contract, tdgen.dorg.disc.structure, tdgen.dorg.disc.pr.book, tdgen.dorg.extern |
| `dorg_9` | `integer` | `int` |  |  | Discount Origin. Allowed values: 2, 4, 6, 8, 10, 12 |
| `dorg_kw_9` | `string` | `varchar` |  |  | Discount Origin (keyword). Allowed values: tdgen.dorg.not.applicable, tdgen.dorg.manual, tdgen.dorg.contract, tdgen.dorg.disc.structure, tdgen.dorg.disc.pr.book, tdgen.dorg.extern |
| `dorg_10` | `integer` | `int` |  |  | Discount Origin. Allowed values: 2, 4, 6, 8, 10, 12 |
| `dorg_kw_10` | `string` | `varchar` |  |  | Discount Origin (keyword). Allowed values: tdgen.dorg.not.applicable, tdgen.dorg.manual, tdgen.dorg.contract, tdgen.dorg.disc.structure, tdgen.dorg.disc.pr.book, tdgen.dorg.extern |
| `dorg_11` | `integer` | `int` |  |  | Discount Origin. Allowed values: 2, 4, 6, 8, 10, 12 |
| `dorg_kw_11` | `string` | `varchar` |  |  | Discount Origin (keyword). Allowed values: tdgen.dorg.not.applicable, tdgen.dorg.manual, tdgen.dorg.contract, tdgen.dorg.disc.structure, tdgen.dorg.disc.pr.book, tdgen.dorg.extern |
| `dmty_1` | `integer` | `int` |  |  | Discount Matrix Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 21, 22, 100 |
| `dmty_kw_1` | `string` | `varchar` |  |  | Discount Matrix Type (keyword). Allowed values: tdpcg.maty.sobook, tdpcg.maty.soldsc, tdpcg.maty.sodsc, tdpcg.maty.pobook, tdpcg.maty.poldsc, tdpcg.maty.podsc, tdpcg.maty.transfer, tdpcg.maty.lineprom, tdpcg.maty.ordprom, tdpcg.maty.clfrate, tdpcg.maty.cafrate, tdpcg.maty.srbook, tdpcg.maty.srldsc, tdpcg.maty.srdsc, tdpcg.maty.none |
| `dmty_2` | `integer` | `int` |  |  | Discount Matrix Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 21, 22, 100 |
| `dmty_kw_2` | `string` | `varchar` |  |  | Discount Matrix Type (keyword). Allowed values: tdpcg.maty.sobook, tdpcg.maty.soldsc, tdpcg.maty.sodsc, tdpcg.maty.pobook, tdpcg.maty.poldsc, tdpcg.maty.podsc, tdpcg.maty.transfer, tdpcg.maty.lineprom, tdpcg.maty.ordprom, tdpcg.maty.clfrate, tdpcg.maty.cafrate, tdpcg.maty.srbook, tdpcg.maty.srldsc, tdpcg.maty.srdsc, tdpcg.maty.none |
| `dmty_3` | `integer` | `int` |  |  | Discount Matrix Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 21, 22, 100 |
| `dmty_kw_3` | `string` | `varchar` |  |  | Discount Matrix Type (keyword). Allowed values: tdpcg.maty.sobook, tdpcg.maty.soldsc, tdpcg.maty.sodsc, tdpcg.maty.pobook, tdpcg.maty.poldsc, tdpcg.maty.podsc, tdpcg.maty.transfer, tdpcg.maty.lineprom, tdpcg.maty.ordprom, tdpcg.maty.clfrate, tdpcg.maty.cafrate, tdpcg.maty.srbook, tdpcg.maty.srldsc, tdpcg.maty.srdsc, tdpcg.maty.none |
| `dmty_4` | `integer` | `int` |  |  | Discount Matrix Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 21, 22, 100 |
| `dmty_kw_4` | `string` | `varchar` |  |  | Discount Matrix Type (keyword). Allowed values: tdpcg.maty.sobook, tdpcg.maty.soldsc, tdpcg.maty.sodsc, tdpcg.maty.pobook, tdpcg.maty.poldsc, tdpcg.maty.podsc, tdpcg.maty.transfer, tdpcg.maty.lineprom, tdpcg.maty.ordprom, tdpcg.maty.clfrate, tdpcg.maty.cafrate, tdpcg.maty.srbook, tdpcg.maty.srldsc, tdpcg.maty.srdsc, tdpcg.maty.none |
| `dmty_5` | `integer` | `int` |  |  | Discount Matrix Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 21, 22, 100 |
| `dmty_kw_5` | `string` | `varchar` |  |  | Discount Matrix Type (keyword). Allowed values: tdpcg.maty.sobook, tdpcg.maty.soldsc, tdpcg.maty.sodsc, tdpcg.maty.pobook, tdpcg.maty.poldsc, tdpcg.maty.podsc, tdpcg.maty.transfer, tdpcg.maty.lineprom, tdpcg.maty.ordprom, tdpcg.maty.clfrate, tdpcg.maty.cafrate, tdpcg.maty.srbook, tdpcg.maty.srldsc, tdpcg.maty.srdsc, tdpcg.maty.none |
| `dmty_6` | `integer` | `int` |  |  | Discount Matrix Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 21, 22, 100 |
| `dmty_kw_6` | `string` | `varchar` |  |  | Discount Matrix Type (keyword). Allowed values: tdpcg.maty.sobook, tdpcg.maty.soldsc, tdpcg.maty.sodsc, tdpcg.maty.pobook, tdpcg.maty.poldsc, tdpcg.maty.podsc, tdpcg.maty.transfer, tdpcg.maty.lineprom, tdpcg.maty.ordprom, tdpcg.maty.clfrate, tdpcg.maty.cafrate, tdpcg.maty.srbook, tdpcg.maty.srldsc, tdpcg.maty.srdsc, tdpcg.maty.none |
| `dmty_7` | `integer` | `int` |  |  | Discount Matrix Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 21, 22, 100 |
| `dmty_kw_7` | `string` | `varchar` |  |  | Discount Matrix Type (keyword). Allowed values: tdpcg.maty.sobook, tdpcg.maty.soldsc, tdpcg.maty.sodsc, tdpcg.maty.pobook, tdpcg.maty.poldsc, tdpcg.maty.podsc, tdpcg.maty.transfer, tdpcg.maty.lineprom, tdpcg.maty.ordprom, tdpcg.maty.clfrate, tdpcg.maty.cafrate, tdpcg.maty.srbook, tdpcg.maty.srldsc, tdpcg.maty.srdsc, tdpcg.maty.none |
| `dmty_8` | `integer` | `int` |  |  | Discount Matrix Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 21, 22, 100 |
| `dmty_kw_8` | `string` | `varchar` |  |  | Discount Matrix Type (keyword). Allowed values: tdpcg.maty.sobook, tdpcg.maty.soldsc, tdpcg.maty.sodsc, tdpcg.maty.pobook, tdpcg.maty.poldsc, tdpcg.maty.podsc, tdpcg.maty.transfer, tdpcg.maty.lineprom, tdpcg.maty.ordprom, tdpcg.maty.clfrate, tdpcg.maty.cafrate, tdpcg.maty.srbook, tdpcg.maty.srldsc, tdpcg.maty.srdsc, tdpcg.maty.none |
| `dmty_9` | `integer` | `int` |  |  | Discount Matrix Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 21, 22, 100 |
| `dmty_kw_9` | `string` | `varchar` |  |  | Discount Matrix Type (keyword). Allowed values: tdpcg.maty.sobook, tdpcg.maty.soldsc, tdpcg.maty.sodsc, tdpcg.maty.pobook, tdpcg.maty.poldsc, tdpcg.maty.podsc, tdpcg.maty.transfer, tdpcg.maty.lineprom, tdpcg.maty.ordprom, tdpcg.maty.clfrate, tdpcg.maty.cafrate, tdpcg.maty.srbook, tdpcg.maty.srldsc, tdpcg.maty.srdsc, tdpcg.maty.none |
| `dmty_10` | `integer` | `int` |  |  | Discount Matrix Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 21, 22, 100 |
| `dmty_kw_10` | `string` | `varchar` |  |  | Discount Matrix Type (keyword). Allowed values: tdpcg.maty.sobook, tdpcg.maty.soldsc, tdpcg.maty.sodsc, tdpcg.maty.pobook, tdpcg.maty.poldsc, tdpcg.maty.podsc, tdpcg.maty.transfer, tdpcg.maty.lineprom, tdpcg.maty.ordprom, tdpcg.maty.clfrate, tdpcg.maty.cafrate, tdpcg.maty.srbook, tdpcg.maty.srldsc, tdpcg.maty.srdsc, tdpcg.maty.none |
| `dmty_11` | `integer` | `int` |  |  | Discount Matrix Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 21, 22, 100 |
| `dmty_kw_11` | `string` | `varchar` |  |  | Discount Matrix Type (keyword). Allowed values: tdpcg.maty.sobook, tdpcg.maty.soldsc, tdpcg.maty.sodsc, tdpcg.maty.pobook, tdpcg.maty.poldsc, tdpcg.maty.podsc, tdpcg.maty.transfer, tdpcg.maty.lineprom, tdpcg.maty.ordprom, tdpcg.maty.clfrate, tdpcg.maty.cafrate, tdpcg.maty.srbook, tdpcg.maty.srldsc, tdpcg.maty.srdsc, tdpcg.maty.none |
| `dmde_1` | `string` | `varchar` |  |  | Discount Matrix. Max length: 9 |
| `dmde_2` | `string` | `varchar` |  |  | Discount Matrix. Max length: 9 |
| `dmde_3` | `string` | `varchar` |  |  | Discount Matrix. Max length: 9 |
| `dmde_4` | `string` | `varchar` |  |  | Discount Matrix. Max length: 9 |
| `dmde_5` | `string` | `varchar` |  |  | Discount Matrix. Max length: 9 |
| `dmde_6` | `string` | `varchar` |  |  | Discount Matrix. Max length: 9 |
| `dmde_7` | `string` | `varchar` |  |  | Discount Matrix. Max length: 9 |
| `dmde_8` | `string` | `varchar` |  |  | Discount Matrix. Max length: 9 |
| `dmde_9` | `string` | `varchar` |  |  | Discount Matrix. Max length: 9 |
| `dmde_10` | `string` | `varchar` |  |  | Discount Matrix. Max length: 9 |
| `dmde_11` | `string` | `varchar` |  |  | Discount Matrix. Max length: 9 |
| `dmse_1` | `integer` | `int` |  |  | Discount Matrix Sequence |
| `dmse_2` | `integer` | `int` |  |  | Discount Matrix Sequence |
| `dmse_3` | `integer` | `int` |  |  | Discount Matrix Sequence |
| `dmse_4` | `integer` | `int` |  |  | Discount Matrix Sequence |
| `dmse_5` | `integer` | `int` |  |  | Discount Matrix Sequence |
| `dmse_6` | `integer` | `int` |  |  | Discount Matrix Sequence |
| `dmse_7` | `integer` | `int` |  |  | Discount Matrix Sequence |
| `dmse_8` | `integer` | `int` |  |  | Discount Matrix Sequence |
| `dmse_9` | `integer` | `int` |  |  | Discount Matrix Sequence |
| `dmse_10` | `integer` | `int` |  |  | Discount Matrix Sequence |
| `dmse_11` | `integer` | `int` |  |  | Discount Matrix Sequence |
| `dmth_1` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_1` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_2` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_2` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_3` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_3` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_4` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_4` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_5` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_5` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_6` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_6` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_7` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_7` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_8` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_8` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_9` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_9` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_10` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_10` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `dmth_11` | `integer` | `int` |  |  | Discount Method. Allowed values: 1, 2 |
| `dmth_kw_11` | `string` | `varchar` |  |  | Discount Method (keyword). Allowed values: tddmth.gross, tddmth.net |
| `cdis_1` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_2` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_3` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_4` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_5` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_6` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_7` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_8` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_9` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_10` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `cdis_11` | `string` | `varchar` |  |  | Discount Code. Max length: 3 |
| `dtrm` | `integer` | `int` |  |  | Determining. Allowed values: 1, 2 |
| `dtrm_kw` | `string` | `varchar` |  |  | Determining (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `elgb` | `integer` | `int` |  |  | Eligible. Allowed values: 1, 2 |
| `elgb_kw` | `string` | `varchar` |  |  | Eligible (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `stdc` | `float` | `float` |  |  | Structure Discount |
| `oamt` | `float` | `float` |  |  | Order Amount |
| `rcno` | `string` | `varchar` |  |  | Receipt Number. Max length: 9 |
| `rseq` | `integer` | `int` |  |  | Receipt Line Number |
| `dino__en_us` | `string` | `varchar` |  | `not_null` | (required) Packing Slip - base language. Max length: 30 |
| `qpps` | `float` | `float` |  |  | Packing Slip Quantity in Piece Unit |
| `qips` | `float` | `float` |  |  | Packing Slip Quantity |
| `qpdl` | `float` | `float` |  |  | Received Quantity in Piece Unit |
| `qidl` | `float` | `float` |  |  | Received Quantity |
| `qpap` | `float` | `float` |  |  | Approved Quantity in Piece Unit |
| `qiap` | `float` | `float` |  |  | Approved Quantity |
| `crej` | `string` | `varchar` |  |  | Reason for Rejection. Max length: 6 |
| `qprj` | `float` | `float` |  |  | Rejected Quantity in Piece Unit |
| `qirj` | `float` | `float` |  |  | Rejected Quantity |
| `qpbo` | `float` | `float` |  |  | Backorder Quantity in Piece Unit |
| `cupb` | `string` | `varchar` |  |  | Backorder Piece Unit. Max length: 3 |
| `cvpb` | `float` | `float` |  |  | Backorder Piece Unit Conversion Factor |
| `qibo` | `float` | `float` |  |  | Backorder Quantity |
| `qpbc` | `float` | `float` |  |  | Backorder Quantity to be Confirmed in Piece Unit |
| `qbbc` | `float` | `float` |  |  | Backorder Quantity to be Confirmed |
| `cubp` | `string` | `varchar` |  |  | Backorder Unit. Max length: 3 |
| `cvbp` | `float` | `float` |  |  | Conversion Factor Backorder to Inventory Unit |
| `fire` | `integer` | `int` |  |  | Final Receipt. Allowed values: 1, 2 |
| `fire_kw` | `string` | `varchar` |  |  | Final Receipt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `qpbp` | `float` | `float` |  |  | Potential Backorder Quantity in Piece unit |
| `qibp` | `float` | `float` |  |  | Potential Backorder Quantity |
| `ddon` | `integer` | `int` |  |  | Number of Deliveries Done |
| `lseq` | `integer` | `int` |  |  | Linked Backorder Sequence |
| `pseq` | `integer` | `int` |  |  | Parent Sequence |
| `amld` | `float` | `float` |  |  | Order Line Discount Amount |
| `amod` | `float` | `float` |  |  | Order Discount Amount |
| `damt` | `float` | `float` |  |  | Receipt Amount |
| `stsc` | `integer` | `int` |  |  | Update Status of Actual Purchase Costs. Allowed values: 1, 2 |
| `stsc_kw` | `string` | `varchar` |  |  | Update Status of Actual Purchase Costs (keyword). Allowed values: tcstsc.free, tcstsc.updated |
| `stsd` | `integer` | `int` |  |  | Invoicing Status. Allowed values: 1, 2, 3, 4 |
| `stsd_kw` | `string` | `varchar` |  |  | Invoicing Status (keyword). Allowed values: tcstsd.free, tcstsd.approved, tcstsd.all.approved, tcstsd.matched |
| `vryn` | `integer` | `int` |  |  | Vendor Rating. Allowed values: 1, 2 |
| `vryn_kw` | `string` | `varchar` |  |  | Vendor Rating (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `invn` | `string` | `varchar` |  |  | Invoice. Max length: 9 |
| `invd` | `timestamp` | `timestamp_ntz` |  |  | Invoice Date |
| `copr_1` | `float` | `float` |  |  | Standard Cost |
| `copr_2` | `float` | `float` |  |  | Standard Cost |
| `copr_3` | `float` | `float` |  |  | Standard Cost |
| `coop_1` | `float` | `float` |  |  | Operation Costs |
| `coop_2` | `float` | `float` |  |  | Operation Costs |
| `coop_3` | `float` | `float` |  |  | Operation Costs |
| `cpcp` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `cadr` | `string` | `varchar` |  |  | Warehouse Address. Max length: 9 |
| `lsel` | `integer` | `int` |  |  | Lot Selection. Allowed values: 1, 2, 3 |
| `lsel_kw` | `string` | `varchar` |  |  | Lot Selection (keyword). Allowed values: tclsel.any, tclsel.same, tclsel.specific |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `serl` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `cstl` | `string` | `varchar` |  |  | Extension. Max length: 4 |
| `ccco` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `ctrj` | `string` | `varchar` |  |  | Route. Max length: 5 |
| `akcd` | `string` | `varchar` |  |  | Purchase Acknowledgment. Max length: 2 |
| `cfrw` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `crbn` | `integer` | `int` |  |  | Carrier Binding. Allowed values: 1, 2 |
| `crbn_kw` | `string` | `varchar` |  |  | Carrier Binding (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `clyn` | `integer` | `int` |  |  | Canceled. Allowed values: 1, 2 |
| `clyn_kw` | `string` | `varchar` |  |  | Canceled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `clip` | `integer` | `int` |  |  | Cancellation in Process. Allowed values: 1, 2 |
| `clip_kw` | `string` | `varchar` |  |  | Cancellation in Process (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cpva` | `integer` | `int` |  |  | Product Variant |
| `crcd` | `string` | `varchar` |  |  | Change Reason. Max length: 6 |
| `ctcd` | `string` | `varchar` |  |  | Change Type. Max length: 6 |
| `cosn` | `string` | `varchar` |  |  | Change Order Sequence Number. Max length: 8 |
| `qpiv` | `float` | `float` |  |  | Invoiced Quantity in Piece Unit |
| `qiiv` | `float` | `float` |  |  | Invoiced Quantity |
| `iamt` | `float` | `float` |  |  | Invoice Amount |
| `comm` | `integer` | `int` |  |  | For Commingling. Allowed values: 1, 2 |
| `comm_kw` | `string` | `varchar` |  |  | For Commingling (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `appr` | `integer` | `int` |  |  | Approved. Allowed values: 1, 2 |
| `appr_kw` | `string` | `varchar` |  |  | Approved (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `bptc` | `string` | `varchar` |  |  | Business Partner Tax Country. Max length: 3 |
| `exmt` | `integer` | `int` |  |  | Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ceno` | `string` | `varchar` |  |  | Exempt Certificate. Max length: 24 |
| `rcod` | `string` | `varchar` |  |  | Exempt Reason. Max length: 6 |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `gefo` | `integer` | `int` |  |  | Generate Freight Orders from Purchase. Allowed values: 1, 2 |
| `gefo_kw` | `string` | `varchar` |  |  | Generate Freight Orders from Purchase (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ldat` | `timestamp` | `timestamp_ntz` |  |  | Planned Load Date |
| `serv` | `string` | `varchar` |  |  | Freight Service Level. Max length: 3 |
| `casi` | `string` | `varchar` |  |  | Additional Statistical Information Set. Max length: 3 |
| `ptpe` | `string` | `varchar` |  |  | Purchase Type. Max length: 6 |
| `glco` | `string` | `varchar` |  |  | General Ledger. Max length: 135 |
| `sbim` | `integer` | `int` |  |  | Self-Billing. Allowed values: 1, 2 |
| `sbim_kw` | `string` | `varchar` |  |  | Self-Billing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `paft` | `integer` | `int` |  |  | Invoice after. Allowed values: 1, 2 |
| `paft_kw` | `string` | `varchar` |  |  | Invoice after (keyword). Allowed values: tcpaft.approval, tcpaft.receipts |
| `sbmt` | `string` | `varchar` |  |  | Self-Billing Method. Max length: 3 |
| `cuva` | `float` | `float` |  |  | Customs Value |
| `cono` | `string` | `varchar` |  |  | Contract. Max length: 9 |
| `cpon` | `integer` | `int` |  |  | Contract Position |
| `ccof` | `string` | `varchar` |  |  | Contract Purchase Office. Max length: 6 |
| `csqn` | `integer` | `int` |  |  | Contract Sequence |
| `cnig` | `integer` | `int` |  |  | Contract Ignored. Allowed values: 1, 2 |
| `cnig_kw` | `string` | `varchar` |  |  | Contract Ignored (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `paya` | `string` | `varchar` |  |  | Payment Agreement. Max length: 3 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `pmnt` | `integer` | `int` |  |  | Payment. Allowed values: 10, 20, 30, 90 |
| `pmnt_kw` | `string` | `varchar` |  |  | Payment (keyword). Allowed values: tcpmnt.pay.on.receipt, tcpmnt.pay.on.use, tcpmnt.no.payment, tcpmnt.not.applicable |
| `pmni` | `integer` | `int` |  |  | Payment (Internally). Allowed values: 10, 20, 30, 90 |
| `pmni_kw` | `string` | `varchar` |  |  | Payment (Internally) (keyword). Allowed values: tcpmnt.pay.on.receipt, tcpmnt.pay.on.use, tcpmnt.no.payment, tcpmnt.not.applicable |
| `pmnd` | `integer` | `int` |  |  | Payment (Direct Delivery). Allowed values: 10, 20, 30, 90 |
| `pmnd_kw` | `string` | `varchar` |  |  | Payment (Direct Delivery) (keyword). Allowed values: tcpmnt.pay.on.receipt, tcpmnt.pay.on.use, tcpmnt.no.payment, tcpmnt.not.applicable |
| `owns` | `integer` | `int` |  |  | Return Ownership. Allowed values: 10, 20, 30, 40, 50, 90 |
| `owns_kw` | `string` | `varchar` |  |  | Return Ownership (keyword). Allowed values: tcowns.comp.owned, tcowns.consigned, tcowns.cust.owned, tcowns.return.as.issue, tcowns.deferred, tcowns.not.appl |
| `ownr` | `string` | `varchar` |  |  | Owner. Max length: 9 |
| `lccl` | `string` | `varchar` |  |  | Landed Costs Classification. Max length: 12 |
| `spcn` | `string` | `varchar` |  |  | Specification. Max length: 22 |
| `bgxc` | `integer` | `int` |  |  | Budget Exception. Allowed values: 1, 2 |
| `bgxc_kw` | `string` | `varchar` |  |  | Budget Exception (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pegd` | `integer` | `int` |  |  | Peg Distribution. Allowed values: 1, 2 |
| `pegd_kw` | `string` | `varchar` |  |  | Peg Distribution (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `issp` | `integer` | `int` |  |  | Invoice by Stage Payments. Allowed values: 1, 2 |
| `issp_kw` | `string` | `varchar` |  |  | Invoice by Stage Payments (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `spss` | `string` | `varchar` |  |  | Stage Payment Schedule Set. Max length: 3 |
| `bkyn` | `integer` | `int` |  |  | Blocked. Allowed values: 1, 2 |
| `bkyn_kw` | `string` | `varchar` |  |  | Blocked (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `lcrq` | `integer` | `int` |  |  | Letter of Credit Required. Allowed values: 1, 2 |
| `lcrq_kw` | `string` | `varchar` |  |  | Letter of Credit Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bgrq` | `integer` | `int` |  |  | Bank Guarantee - Applicant Required. Allowed values: 1, 2 |
| `bgrq_kw` | `string` | `varchar` |  |  | Bank Guarantee - Applicant Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bgrb` | `integer` | `int` |  |  | Bank Guarantee - Beneficiary Required. Allowed values: 1, 2 |
| `bgrb_kw` | `string` | `varchar` |  |  | Bank Guarantee - Beneficiary Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `etpc` | `integer` | `int` |  |  | Exclude from Target Price Calculation. Allowed values: 1, 2 |
| `etpc_kw` | `string` | `varchar` |  |  | Exclude from Target Price Calculation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tpbk` | `string` | `varchar` |  |  | Target Price Book. Max length: 9 |
| `tpbl` | `integer` | `int` |  |  | Target Price Book Line |
| `tapr` | `float` | `float` |  |  | Target Price |
| `sclb` | `integer` | `int` |  |  | Synced to Collaboration. Allowed values: 5, 10, 15, 99 |
| `sclb_kw` | `string` | `varchar` |  |  | Synced to Collaboration (keyword). Allowed values: tcgen.sclb.yes, tcgen.sclb.no, tcgen.sclb.partially.synce, tcgen.sclb.not.appl |
| `rsta` | `integer` | `int` |  |  | Response Status. Allowed values: 5, 10, 20, 30, 40, 50, 255 |
| `rsta_kw` | `string` | `varchar` |  |  | Response Status (keyword). Allowed values: tcspd.rsta.applicable, tcspd.rsta.pend.acceptance, tcspd.rsta.accepted, tcspd.rsta.declined, tcspd.rsta.partially.accep, tcspd.rsta.partially.decli, tcspd.rsta.not.appl |
| `lnst` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 5, 10 |
| `lnst_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tdgen.lnst.open, tdgen.lnst.incomplete |
| `taxp` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2 |
| `taxp_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `cpcl` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `cpln` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `csgp` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `acti__en_us` | `string` | `varchar` |  | `not_null` | (required) Obsolete - base language. Max length: 20 |
| `pmsk` | `string` | `varchar` |  |  | Obsolete. Max length: 20 |
| `txta` | `integer` | `int` |  |  | Order Line Text |
| `btx1` | `integer` | `int` |  |  | Negotiation Notes |
| `btx2` | `integer` | `int` |  |  | Backorder Text |
| `cdf_cdsc__en_us` | `string` | `varchar` |  | `not_null` | (required) Sub-Category Description - base language. Max length: 250 |
| `cdf_cono` | `string` | `varchar` |  |  | Contract Number. Max length: 9 |
| `cdf_cpon` | `integer` | `int` |  |  | Contract Position |
| `cdf_itmd__en_us` | `string` | `varchar` |  | `not_null` | (required) Item Detail - base language. Max length: 100 |
| `cdf_sctg` | `string` | `varchar` |  |  | Sub-Category. Max length: 25 |
| `orno_cosn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur414 Purchase Change Order Sequence Numbers |
| `orno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur400 Purchase Orders |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom121 Ship-from Business Partners |
| `sfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `sfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `sfsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu001 Item - Purchase |
| `atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd030 Attribute Sets |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `citt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd006 Item Code Systems |
| `mpnr_cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu045 Manufacturer Part Numbers |
| `subs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `mpsn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu046 Manufacturer Part Number Sets |
| `cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs060 Manufacturers |
| `cupq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cuqp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `prsg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs212 Price Stages |
| `cupp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `crej_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `cupb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cubp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cpcp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `ccco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `ctrj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs004 Routes |
| `akcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur054 Purchase Acknowledgments |
| `cfrw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `crcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur097 Change Reasons |
| `ctcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur098 Change Types |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `ccty_cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs036 Tax Codes by Country |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `rcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `serv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs075 Freight Service Levels |
| `casi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom705 Extra Intrastat Info |
| `ptpe_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs201 Purchase Types |
| `sbmt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs057 Self-Billing Methods |
| `paya_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs206 Payment Agreements |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `ownr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `lccl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tclct020 Landed Costs Classifications |
| `spss_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs240 Installment Schedules Sets |
| `tpbk_tpbl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpcg033 Target Price Book Lines |
| `tpbk_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpcg013 Target Price Books |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `btx1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `btx2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `cdf_cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur300 Purchase Contracts |
| `qoor_invu` | `float` | `float` |  |  | CC: Ordered Quantity in Inventory Unit |
| `qoor_buar` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Area |
| `qoor_buln` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Length |
| `qoor_bupc` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Piece |
| `qoor_butm` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Time |
| `qoor_buvl` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Volume |
| `qoor_buwg` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Weight |
| `qidl_trnu` | `float` | `float` |  |  | CC: Received Quantity in Transaction Unit |
| `qidl_buar` | `float` | `float` |  |  | CC: Received Quantity in Base Unit Area |
| `qidl_buln` | `float` | `float` |  |  | CC: Received Quantity in Base Unit Length |
| `qidl_bupc` | `float` | `float` |  |  | CC: Received Quantity in Base Unit Piece |
| `qidl_butm` | `float` | `float` |  |  | CC: Received Quantity in Base Unit Time |
| `qidl_buvl` | `float` | `float` |  |  | CC: Received Quantity in Base Unit Volume |
| `qidl_buwg` | `float` | `float` |  |  | CC: Received Quantity in Base Unit Weight |
| `qiap_trnu` | `float` | `float` |  |  | CC: Approved Quantity in Transaction Unit |
| `qiap_buar` | `float` | `float` |  |  | CC: Approved Quantity in Base Unit Area |
| `qiap_buln` | `float` | `float` |  |  | CC: Approved Quantity in Base Unit Length |
| `qiap_bupc` | `float` | `float` |  |  | CC: Approved Quantity in Base Unit Piece |
| `qiap_butm` | `float` | `float` |  |  | CC: Approved Quantity in Base Unit Time |
| `qiap_buvl` | `float` | `float` |  |  | CC: Approved Quantity in Base Unit Volume |
| `qiap_buwg` | `float` | `float` |  |  | CC: Approved Quantity in Base Unit Weight |
| `qirj_trnu` | `float` | `float` |  |  | CC: Rejected Quantity in Transaction Unit |
| `qirj_buar` | `float` | `float` |  |  | CC: Rejected Quantity in Base Unit Area |
| `qirj_buln` | `float` | `float` |  |  | CC: Rejected Quantity in Base Unit Length |
| `qirj_bupc` | `float` | `float` |  |  | CC: Rejected Quantity in Base Unit Piece |
| `qirj_butm` | `float` | `float` |  |  | CC: Rejected Quantity in Base Unit Time |
| `qirj_buvl` | `float` | `float` |  |  | CC: Rejected Quantity in Base Unit Volume |
| `qirj_buwg` | `float` | `float` |  |  | CC: Rejected Quantity in Base Unit Weight |
| `qibo_invu` | `float` | `float` |  |  | CC: Back Order Quantity in Inventory Unit |
| `qibo_trnu` | `float` | `float` |  |  | CC: Back Order Quantity in Transaction Unit |
| `qibo_buar` | `float` | `float` |  |  | CC: Back Order Quantity in Base Unit Area |
| `qibo_buln` | `float` | `float` |  |  | CC: Back Order Quantity in Base Unit Length |
| `qibo_bupc` | `float` | `float` |  |  | CC: Back Order Quantity in Base Unit Piece |
| `qibo_butm` | `float` | `float` |  |  | CC: Back Order Quantity in Base Unit Time |
| `qibo_buvl` | `float` | `float` |  |  | CC: Back Order Quantity in Base Unit Volume |
| `qibo_buwg` | `float` | `float` |  |  | CC: Back Order Quantity in Base Unit Weight |
| `qiiv_trnu` | `float` | `float` |  |  | CC: Invoiced Quantity in Transaction Unit |
| `qiiv_buar` | `float` | `float` |  |  | CC: Invoiced Quantity in Base Unit Area |
| `qiiv_buln` | `float` | `float` |  |  | CC: Invoiced Quantity in Base Unit Length |
| `qiiv_bupc` | `float` | `float` |  |  | CC: Invoiced Quantity in Base Unit Piece |
| `qiiv_butm` | `float` | `float` |  |  | CC: Invoiced Quantity in Base Unit Time |
| `qiiv_buvl` | `float` | `float` |  |  | CC: Invoiced Quantity in Base Unit Volume |
| `qiiv_buwg` | `float` | `float` |  |  | CC: Invoiced Quantity in Base Unit Weight |
| `lcam_trnc` | `float` | `float` |  |  | CC: Total Landed Cost |
| `copr_trnc` | `float` | `float` |  |  | CC: Standard Costs in Transaction Currency |
| `coop_trnc` | `float` | `float` |  |  | CC: Operation Costs in Transaction Currency |
| `qbbc_invu` | `float` | `float` |  |  | CC: Back Order Quantity to be Confirmed in Inventory Unit |
| `qbbc_trnu` | `float` | `float` |  |  | CC: Back Order Quantity to be Confirmed in Transaction Unit |
| `qbbc_buar` | `float` | `float` |  |  | CC: Back Order Quantity to be Confirmed in Base Unit Area |
| `qbbc_buln` | `float` | `float` |  |  | CC: Back Order Quantity to be Confirmed in Base Unit Length |
| `qbbc_bupc` | `float` | `float` |  |  | CC: Back Order Quantity to be Confirmed in Base Unit Piece |
| `qbbc_butm` | `float` | `float` |  |  | CC: Back Order Quantity to be Confirmed in Base Unit Time |
| `qbbc_buvl` | `float` | `float` |  |  | CC: Back Order Quantity to be Confirmed in Base Unit Volume |
| `qbbc_buwg` | `float` | `float` |  |  | CC: Back Order Quantity to be Confirmed in Base Unit Weight |
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

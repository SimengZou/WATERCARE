# tdsls401

LN tdsls401 Sales Order Lines table, Generated 2026-04-10T19:41:26Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdsls401` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orno`, `pono`, `sqnb` |
| **Column count** | 478 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Sales Order. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `sqnb` | `integer` | `int` | `PK` | `not_null` | (required) Sequence Number |
| `ofbp` | `string` | `varchar` |  |  | Sold-to Business Partner. Max length: 9 |
| `dltp` | `integer` | `int` |  |  | Delivery Type. Allowed values: 1, 5, 10, 15, 20, 25, 30, 40, 45 |
| `dltp_kw` | `string` | `varchar` |  |  | Delivery Type (keyword). Allowed values: tdsls.dltp.not.applicable, tdsls.dltp.direct.delivery, tdsls.dltp.cross.docking, tdsls.dltp.warehousing, tdsls.dltp.work.center, tdsls.dltp.sales, tdsls.dltp.invoicing, tdsls.dltp.purchase, tdsls.dltp.production |
| `subc` | `integer` | `int` |  |  | Contains Customer Furnished Material. Allowed values: 1, 2 |
| `subc_kw` | `string` | `varchar` |  |  | Contains Customer Furnished Material (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `atse` | `string` | `varchar` |  |  | Attribute Set. Max length: 35 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `cpva` | `integer` | `int` |  |  | Product Variant |
| `olid` | `string` | `varchar` |  |  | Configuration. Max length: 9 |
| `opol` | `integer` | `int` |  |  | Make Customized. Allowed values: 1, 2 |
| `opol_kw` | `string` | `varchar` |  |  | Make Customized (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sdsc` | `integer` | `int` |  |  | Standard Description. Allowed values: 1, 2 |
| `sdsc_kw` | `string` | `varchar` |  |  | Standard Description (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pric` | `float` | `float` |  |  | Price |
| `prsg` | `string` | `varchar` |  |  | Price Stage. Max length: 3 |
| `porg` | `integer` | `int` |  |  | Price Origin. Allowed values: 2, 4, 6, 8, 10, 12, 13, 14, 16, 18, 20, 25, 30, 35, 40 |
| `porg_kw` | `string` | `varchar` |  |  | Price Origin (keyword). Allowed values: tdgen.porg.not.applicable, tdgen.porg.manual, tdgen.porg.contract, tdgen.porg.variant, tdgen.porg.item.pur, tdgen.porg.item.sls, tdgen.porg.item.service, tdgen.porg.supp.pr.book, tdgen.porg.def.pr.book, tdgen.porg.price.structure, tdgen.porg.extern, tdgen.porg.consumption, tdgen.porg.generic.pr.list, tdgen.porg.project.budget, tdgen.porg.projectcost.obj |
| `pmde` | `string` | `varchar` |  |  | Price Matrix. Max length: 9 |
| `pmse` | `integer` | `int` |  |  | Price Matrix Sequence |
| `pror` | `integer` | `int` |  |  | Default Price Book. Allowed values: 1, 2 |
| `pror_kw` | `string` | `varchar` |  |  | Default Price Book (keyword). Allowed values: tdpcg.pror.sales, tdpcg.pror.service |
| `cups` | `string` | `varchar` |  |  | Sales Price Unit. Max length: 3 |
| `cvps` | `float` | `float` |  |  | Price Unit Conversion Factor |
| `odat` | `timestamp` | `timestamp_ntz` |  |  | Order Date |
| `pcod` | `string` | `varchar` |  |  | Delivery Pattern. Max length: 6 |
| `ddta` | `timestamp` | `timestamp_ntz` |  |  | Planned Delivery Date |
| `ddtb` | `timestamp` | `timestamp_ntz` |  |  | Initial Requested Delivery Date |
| `ddtc` | `timestamp` | `timestamp_ntz` |  |  | Customer Requested Delivery Date |
| `ddtd` | `timestamp` | `timestamp_ntz` |  |  | Original Promised Delivery Date |
| `ddch` | `integer` | `int` |  |  | Planned Delivery Date Changes |
| `rdta` | `timestamp` | `timestamp_ntz` |  |  | Release Date |
| `qpor` | `float` | `float` |  |  | Ordered Quantity in Piece Unit |
| `cupq` | `string` | `varchar` |  |  | Order Piece Unit. Max length: 3 |
| `cvpq` | `float` | `float` |  |  | Order Piece Unit Conversion Factor |
| `qoor` | `float` | `float` |  |  | Ordered Quantity |
| `cuqs` | `string` | `varchar` |  |  | Sales Unit. Max length: 3 |
| `cvqs` | `float` | `float` |  |  | Order Unit Conversion Factor |
| `bind` | `integer` | `int` |  |  | Quantity Unit Binding. Allowed values: 1, 2 |
| `bind_kw` | `string` | `varchar` |  |  | Quantity Unit Binding (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mprm` | `integer` | `int` |  |  | Multiple Promotion Method. Allowed values: 1, 2 |
| `mprm_kw` | `string` | `varchar` |  |  | Multiple Promotion Method (keyword). Allowed values: tdsls.mprm.parallel, tdsls.mprm.cumulative |
| `tprd` | `float` | `float` |  |  | Total Promotion Discount |
| `oltp` | `integer` | `int` |  |  | Order Line Type. Allowed values: 1, 2, 3 |
| `oltp_kw` | `string` | `varchar` |  |  | Order Line Type (keyword). Allowed values: tdsls.oltp.total, tdsls.oltp.detail, tdsls.oltp.backorder |
| `qicm` | `float` | `float` |  |  | Committed Quantity |
| `leng` | `float` | `float` |  |  | Length |
| `widt` | `float` | `float` |  |  | Width |
| `thic` | `float` | `float` |  |  | Height |
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
| `oamt` | `float` | `float` |  |  | Amount |
| `dldt` | `timestamp` | `timestamp_ntz` |  |  | Delivery Date |
| `qidl` | `float` | `float` |  |  | Delivered Quantity |
| `qpdl` | `float` | `float` |  |  | Delivered Quantity in Piece Unit |
| `qohb` | `float` | `float` |  |  | Hold Back Quantity |
| `qpbo` | `float` | `float` |  |  | Backorder Quantity in Piece Unit |
| `cupb` | `string` | `varchar` |  |  | Backorder Piece Unit. Max length: 3 |
| `cvpb` | `float` | `float` |  |  | Backorder Piece Unit Conversion Factor |
| `qbbo` | `float` | `float` |  |  | Back Order Quantity |
| `cubs` | `string` | `varchar` |  |  | Backorder Quantity Unit. Max length: 3 |
| `cvbs` | `float` | `float` |  |  | Backorder Quantity Unit Conversion Factor |
| `bqco` | `integer` | `int` |  |  | Back Order Quantity Confirmed. Allowed values: 1, 2 |
| `bqco_kw` | `string` | `varchar` |  |  | Back Order Quantity Confirmed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `lseq` | `integer` | `int` |  |  | Linked Sequence |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `bptc` | `string` | `varchar` |  |  | Business Partner Tax Country. Max length: 3 |
| `exmt` | `integer` | `int` |  |  | Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `cwoc` | `string` | `varchar` |  |  | Work Center. Max length: 6 |
| `isss` | `integer` | `int` |  |  | Inventory Handling. Allowed values: 1, 2 |
| `isss_kw` | `string` | `varchar` |  |  | Inventory Handling (keyword). Allowed values: tcinup.by.main.item, tcinup.by.component |
| `cphl` | `integer` | `int` |  |  | Component Handling. Allowed values: 5, 10, 15 |
| `cphl_kw` | `string` | `varchar` |  |  | Component Handling (keyword). Allowed values: tdcphl.not.applicable, tdcphl.sales.bom, tdcphl.component.lines |
| `revi` | `string` | `varchar` |  |  | Engineering Item Revision. Max length: 6 |
| `lsel` | `integer` | `int` |  |  | Lot Selection. Allowed values: 1, 2, 3 |
| `lsel_kw` | `string` | `varchar` |  |  | Lot Selection (keyword). Allowed values: tclsel.any, tclsel.same, tclsel.specific |
| `clot` | `string` | `varchar` |  |  | Lot. Max length: 20 |
| `ssel` | `integer` | `int` |  |  | Serial Number Selection. Allowed values: 1, 2 |
| `ssel_kw` | `string` | `varchar` |  |  | Serial Number Selection (keyword). Allowed values: tdssel.any, tdssel.specific |
| `serl` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `scon` | `integer` | `int` |  |  | Shipping Constraint. Allowed values: 1, 2, 3, 4, 5, 6, 10 |
| `scon_kw` | `string` | `varchar` |  |  | Shipping Constraint (keyword). Allowed values: tdscon.none, tdscon.sc, tdscon.rc, tdscon.oc, tdscon.sca, tdscon.kc, tdscon.not.applicable |
| `motv` | `string` | `varchar` |  |  | Motive of Transport. Max length: 6 |
| `delc` | `string` | `varchar` |  |  | Delivery Code. Max length: 6 |
| `stbp` | `string` | `varchar` |  |  | Ship-to Business Partner. Max length: 9 |
| `stad` | `string` | `varchar` |  |  | Ship-to Address. Max length: 9 |
| `stcn` | `string` | `varchar` |  |  | Ship-to Contact. Max length: 9 |
| `cfrw` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `dlpt` | `string` | `varchar` |  |  | Delivery Point. Max length: 9 |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `prdt` | `timestamp` | `timestamp_ntz` |  |  | Planned Receipt Date |
| `citt` | `string` | `varchar` |  |  | Item Code System. Max length: 3 |
| `citm__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Item - base language. Max length: 35 |
| `bkyn` | `integer` | `int` |  |  | Blocked. Allowed values: 1, 2 |
| `bkyn_kw` | `string` | `varchar` |  |  | Blocked (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `clyn` | `integer` | `int` |  |  | Canceled. Allowed values: 1, 2 |
| `clyn_kw` | `string` | `varchar` |  |  | Canceled (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `term` | `integer` | `int` |  |  | Terminated. Allowed values: 1, 2 |
| `term_kw` | `string` | `varchar` |  |  | Terminated (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `amod` | `float` | `float` |  |  | Order Discount Amount |
| `rats_1` | `float` | `float` |  |  | Currency Rate Sales |
| `rats_2` | `float` | `float` |  |  | Currency Rate Sales |
| `rats_3` | `float` | `float` |  |  | Currency Rate Sales |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `ratt` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `opri` | `integer` | `int` |  |  | Order Priority |
| `crte` | `string` | `varchar` |  |  | Route. Max length: 5 |
| `scmp` | `integer` | `int` |  |  | Invoice Company |
| `ttyp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `invn` | `integer` | `int` |  |  | Invoice Number |
| `invd` | `timestamp` | `timestamp_ntz` |  |  | Invoice Date |
| `sbim` | `integer` | `int` |  |  | Self-Billing. Allowed values: 1, 2 |
| `sbim_kw` | `string` | `varchar` |  |  | Self-Billing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `chan` | `string` | `varchar` |  |  | Channels. Max length: 3 |
| `corn__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order - base language. Max length: 30 |
| `corp__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order Position - base language. Max length: 16 |
| `cors__en_us` | `string` | `varchar` |  | `not_null` | (required) Customer Order Sequence - base language. Max length: 11 |
| `cosn` | `string` | `varchar` |  |  | Change Order Sequence No.. Max length: 8 |
| `akcd` | `string` | `varchar` |  |  | Sales Acknowledgment. Max length: 2 |
| `crcd` | `string` | `varchar` |  |  | Change Reason. Max length: 6 |
| `ctcd` | `string` | `varchar` |  |  | Change Type. Max length: 6 |
| `amld` | `float` | `float` |  |  | Order Line Discount Amount |
| `ceno` | `string` | `varchar` |  |  | Exempt Certificate. Max length: 24 |
| `rcod` | `string` | `varchar` |  |  | Exempt Reason. Max length: 6 |
| `susi` | `integer` | `int` |  |  | Supplying Company |
| `sfwh` | `string` | `varchar` |  |  | Supply-from Warehouse. Max length: 6 |
| `stsi` | `string` | `varchar` |  |  | Ship-to Site. Max length: 9 |
| `stwh` | `string` | `varchar` |  |  | Ship-to Warehouse. Max length: 6 |
| `oprs` | `integer` | `int` |  |  | Order Promising Status. Allowed values: 1, 2, 3, 4, 5, 6 |
| `oprs_kw` | `string` | `varchar` |  |  | Order Promising Status (keyword). Allowed values: tdsls.oprs.not.accepted, tdsls.oprs.manual, tdsls.oprs.accepted, tdsls.oprs.not.applicable, tdsls.oprs.pending, tdsls.oprs.exception |
| `airp` | `integer` | `int` |  |  | Allow Inventory Recheck for Promised Lines. Allowed values: 1, 2 |
| `airp_kw` | `string` | `varchar` |  |  | Allow Inventory Recheck for Promised Lines (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oref__en_us` | `string` | `varchar` |  | `not_null` | (required) Order Line Reference - base language. Max length: 30 |
| `gefo` | `integer` | `int` |  |  | Generate Freight Order from Sales. Allowed values: 1, 2 |
| `gefo_kw` | `string` | `varchar` |  |  | Generate Freight Order from Sales (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `frin` | `integer` | `int` |  |  | Invoice for Freight. Allowed values: 1, 2 |
| `frin_kw` | `string` | `varchar` |  |  | Invoice for Freight (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `infr` | `integer` | `int` |  |  | Invoice Freight Costs Based On. Allowed values: 5, 10, 15, 20 |
| `infr_kw` | `string` | `varchar` |  |  | Invoice Freight Costs Based On (keyword). Allowed values: tccom.infr.estimate, tccom.infr.actual, tccom.infr.costplus, tccom.infr.not.applic |
| `fram` | `float` | `float` |  |  | Freight Amount |
| `frbn` | `integer` | `int` |  |  | Freight Amount Binding. Allowed values: 1, 2 |
| `frbn_kw` | `string` | `varchar` |  |  | Freight Amount Binding (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crbn` | `integer` | `int` |  |  | Carrier Binding. Allowed values: 1, 2 |
| `crbn_kw` | `string` | `varchar` |  |  | Carrier Binding (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `serv` | `string` | `varchar` |  |  | Freight Service Level. Max length: 3 |
| `clgr` | `string` | `varchar` |  |  | List Group. Max length: 3 |
| `casi` | `string` | `varchar` |  |  | Additional Statistical Information Set. Max length: 3 |
| `ruso` | `integer` | `int` |  |  | Rush Order Line. Allowed values: 1, 2 |
| `ruso_kw` | `string` | `varchar` |  |  | Rush Order Line (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `styp` | `string` | `varchar` |  |  | Sales Type. Max length: 6 |
| `cono` | `string` | `varchar` |  |  | Contract. Max length: 9 |
| `cpon` | `integer` | `int` |  |  | Contract Position Number |
| `ccof` | `string` | `varchar` |  |  | Contract Sales Office. Max length: 6 |
| `cnig` | `integer` | `int` |  |  | Contract Ignored. Allowed values: 1, 2 |
| `cnig_kw` | `string` | `varchar` |  |  | Contract Ignored (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `modi` | `integer` | `int` |  |  | Modified. Allowed values: 1, 2 |
| `modi_kw` | `string` | `varchar` |  |  | Modified (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cuva` | `float` | `float` |  |  | Customs Value |
| `lmbi` | `integer` | `int` |  |  | Link to Monthly Billing Invoicing. Allowed values: 1, 2 |
| `lmbi_kw` | `string` | `varchar` |  |  | Link to Monthly Billing Invoicing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pmnt` | `integer` | `int` |  |  | Payment. Allowed values: 10, 20, 30, 90 |
| `pmnt_kw` | `string` | `varchar` |  |  | Payment (keyword). Allowed values: tcpmnt.pay.on.receipt, tcpmnt.pay.on.use, tcpmnt.no.payment, tcpmnt.not.applicable |
| `owns` | `integer` | `int` |  |  | Return Ownership. Allowed values: 10, 20, 30, 40, 50, 90 |
| `owns_kw` | `string` | `varchar` |  |  | Return Ownership (keyword). Allowed values: tcowns.comp.owned, tcowns.consigned, tcowns.cust.owned, tcowns.return.as.issue, tcowns.deferred, tcowns.not.appl |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `acsl` | `integer` | `int` |  |  | Additional Cost Line. Allowed values: 1, 2 |
| `acsl_kw` | `string` | `varchar` |  |  | Additional Cost Line (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `acti__en_us` | `string` | `varchar` |  | `not_null` | (required) Next Activity - base language. Max length: 20 |
| `spcn` | `string` | `varchar` |  |  | Specification. Max length: 22 |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `lcrq` | `integer` | `int` |  |  | Letter of Credit Required. Allowed values: 1, 2 |
| `lcrq_kw` | `string` | `varchar` |  |  | Letter of Credit Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bgrq` | `integer` | `int` |  |  | Bank Guarantee - Applicant Required. Allowed values: 1, 2 |
| `bgrq_kw` | `string` | `varchar` |  |  | Bank Guarantee - Applicant Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bgrb` | `integer` | `int` |  |  | Bank Guarantee - Beneficiary Required. Allowed values: 1, 2 |
| `bgrb_kw` | `string` | `varchar` |  |  | Bank Guarantee - Beneficiary Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `citg` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `cmnf` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `cpcl` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `cpln` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `csgs` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `copr_1` | `float` | `float` |  |  | Obsolete |
| `copr_2` | `float` | `float` |  |  | Obsolete |
| `copr_3` | `float` | `float` |  |  | Obsolete |
| `cpcp` | `string` | `varchar` |  |  | Obsolete. Max length: 8 |
| `damt` | `float` | `float` |  |  | Obsolete |
| `dlld` | `timestamp` | `timestamp_ntz` |  |  | Obsolete |
| `fcop_1` | `float` | `float` |  |  | Obsolete |
| `fcop_2` | `float` | `float` |  |  | Obsolete |
| `fcop_3` | `float` | `float` |  |  | Obsolete |
| `fxcp_1` | `float` | `float` |  |  | Obsolete |
| `fxcp_2` | `float` | `float` |  |  | Obsolete |
| `fxcp_3` | `float` | `float` |  |  | Obsolete |
| `lssn` | `string` | `varchar` |  |  | Obsolete. Max length: 22 |
| `pcad` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `pcad_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pmsk` | `string` | `varchar` |  |  | Obsolete. Max length: 20 |
| `shln` | `integer` | `int` |  |  | Obsolete |
| `shpm` | `string` | `varchar` |  |  | Obsolete. Max length: 9 |
| `lnst` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 5, 10 |
| `lnst_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tdgen.lnst.open, tdgen.lnst.incomplete |
| `vcop_1` | `float` | `float` |  |  | Obsolete |
| `vcop_2` | `float` | `float` |  |  | Obsolete |
| `vcop_3` | `float` | `float` |  |  | Obsolete |
| `txta` | `integer` | `int` |  |  | Order Line Text |
| `orno_cosn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls414 Sales Change Order Sequence Numbers |
| `orno_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls400 Sales Orders |
| `ofbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom110 Sold-to Business Partners |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `item_site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd150 Items by Site |
| `item_stsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd150 Items by Site |
| `item_atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdisa006 Item Attribute Set - Sales |
| `item_atse_site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd160 Item - Attribute Set by Site |
| `item_atse_stsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd160 Item - Attribute Set by Site |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdisa001 Item - Sales |
| `atse_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd030 Attribute Sets |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `prsg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs212 Price Stages |
| `cups_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `pcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdisa021 Delivery Patterns |
| `cupq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cuqs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cubs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `ccty_cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs036 Tax Codes by Country |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cwoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `motv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `delc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `stbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom111 Ship-to Business Partners |
| `stad_dlpt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom134 Delivery Points |
| `stad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `stcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `cfrw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `citt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd006 Item Code Systems |
| `crte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs004 Routes |
| `chan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs066 Channels |
| `akcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls054 Sales Acknowledgments |
| `crcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls097 Change Reasons |
| `ctcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls098 Change Types |
| `rcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `stsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `stwh_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `serv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs075 Freight Service Levels |
| `clgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd301 List Groups |
| `casi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom705 Extra Intrastat Info |
| `styp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs202 Sales Types |
| `cono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls300 Sales Contracts |
| `ccof_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdsls012 Sales Offices |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `cpva_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Product Variant |
| `qoor_invu` | `float` | `float` |  |  | CC: Ordered Quantity in Inventory Unit |
| `qoor_buar` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Area |
| `qoor_buln` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Length |
| `qoor_bupc` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Piece |
| `qoor_butm` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Time |
| `qoor_buvl` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Volume |
| `qoor_buwg` | `float` | `float` |  |  | CC: Ordered Quantity in Base Unit Weight |
| `qobh_invu` | `float` | `float` |  |  | CC: Hold Back Quantity in Inventory Unit |
| `qohb_buar` | `float` | `float` |  |  | CC: Hold Back Quantity in Base Unit Area |
| `qohb_buln` | `float` | `float` |  |  | CC: Hold Back Quantity in Base Unit Length |
| `qohb_bupc` | `float` | `float` |  |  | CC: Hold Back Quantity in Base Unit Piece |
| `qohb_butm` | `float` | `float` |  |  | CC: Hold Back Quantity in Base Unit Time |
| `qohb_buvl` | `float` | `float` |  |  | CC: Hold Back Quantity in Base Unit Volume |
| `qohb_buwg` | `float` | `float` |  |  | CC: Hold Back Quantity in Base Unit Weight |
| `qidl_trnu` | `float` | `float` |  |  | CC: Delivered Quantity in Transaction Unit |
| `qidl_buar` | `float` | `float` |  |  | CC: Delivered Quantity in Base Unit Area |
| `qidl_buln` | `float` | `float` |  |  | CC: Delivered Quantity in Base Unit Length |
| `qidl_bupc` | `float` | `float` |  |  | CC: Delivered Quantity in Base Unit Piece |
| `qidl_butm` | `float` | `float` |  |  | CC: Delivered Quantity in Base Unit Time |
| `qidl_buvl` | `float` | `float` |  |  | CC: Delivered Quantity in Base Unit Volume |
| `qidl_buwg` | `float` | `float` |  |  | CC: Delivered Quantity in Base Unit Weight |
| `qbbo_trnu` | `float` | `float` |  |  | CC: Back Order Quantity in Transaction Unit |
| `qbbo_buar` | `float` | `float` |  |  | CC: Back Order Quantity in Base Unit Area |
| `qbbo_buln` | `float` | `float` |  |  | CC: Back Order Quantity in Base Unit Length |
| `qbbo_bupc` | `float` | `float` |  |  | CC: Back Order Quantity in Base Unit Piece |
| `qbbo_butm` | `float` | `float` |  |  | CC: Back Order Quantity in Base Unit Time |
| `qbbo_buvl` | `float` | `float` |  |  | CC: Back Order Quantity in Base Unit Volume |
| `qbbo_buwg` | `float` | `float` |  |  | CC: Back Order Quantity in Base Unit Weight |
| `amgr_trnc` | `float` | `float` |  |  | CC: Gross Amount in Transaction Currency |
| `amgr_lclc` | `float` | `float` |  |  | CC: Gross Amount in Local Currency |
| `amgr_rpc1` | `float` | `float` |  |  | CC: Gross Amount in Reporting Currency 1 |
| `amgr_rpc2` | `float` | `float` |  |  | CC: Gross Amount in Reporting Currency 2 |
| `amgr_rfrc` | `float` | `float` |  |  | CC: Gross Amount in Reference Currency |
| `amgr_dtwc` | `float` | `float` |  |  | CC: Gross Amount in Data Warehouse Currency |
| `tprd_lclc` | `float` | `float` |  |  | CC: Order Line Promotion Discount Amount in Local Currency |
| `tprd_rpc1` | `float` | `float` |  |  | CC: Order Line Promotion Discount Amount in Reporting Currency 1 |
| `tprd_rpc2` | `float` | `float` |  |  | CC: Order Line Promotion Discount Amount in Reporting Currency 2 |
| `tprd_rfrc` | `float` | `float` |  |  | CC: Order Line Promotion Discount Amount in Reference Currency |
| `tprd_dtwc` | `float` | `float` |  |  | CC: Order Line Promotion Discount Amount in Data Warehouse Currency |
| `amnt_trnc` | `float` | `float` |  |  | CC: Net Amount in Transaction Currency |
| `amnt_lclc` | `float` | `float` |  |  | CC: Net Amount in Local Currency |
| `amnt_rpc1` | `float` | `float` |  |  | CC: Net Amount in Reporting Currency 1 |
| `amnt_rpc2` | `float` | `float` |  |  | CC: Net Amount in Reporting Currency 2 |
| `amnt_rfrc` | `float` | `float` |  |  | CC: Net Amount in Reference Currency |
| `amnt_dtwc` | `float` | `float` |  |  | CC: Net Amount in Data Warehouse Currency |
| `pric_lclc` | `float` | `float` |  |  | CC: Price in Local Currency |
| `pric_rpc1` | `float` | `float` |  |  | CC: Price in Reporting Currency 1 |
| `pric_rpc2` | `float` | `float` |  |  | CC: Price in Reporting Currency 2 |
| `pric_rfrc` | `float` | `float` |  |  | CC: Price in Reference Currency |
| `pric_dtwc` | `float` | `float` |  |  | CC: Price in Data Warehouse Currency |
| `amod_lclc` | `float` | `float` |  |  | CC: Order Discount Amount in Local Currency |
| `amod_rpc1` | `float` | `float` |  |  | CC: Order Discount Amount in Reporting Currency 1 |
| `amod_rpc2` | `float` | `float` |  |  | CC: Order Discount Amount in Reporting Currency 2 |
| `amod_rfrc` | `float` | `float` |  |  | CC: Order Discount Amount in Reference Currency |
| `amod_dtwc` | `float` | `float` |  |  | CC: Order Discount Amount in Data Warehouse Currency |
| `amld_lclc` | `float` | `float` |  |  | CC: Line Discount Amount in Local Currency |
| `amld_rpc1` | `float` | `float` |  |  | CC: Line Discount Amount in Reporting Currency 1 |
| `amld_rpc2` | `float` | `float` |  |  | CC: Line Discount Amount in Reporting Currency 2 |
| `amld_rfrc` | `float` | `float` |  |  | CC: Line Discount Amount in Reference Currency |
| `amld_dtwc` | `float` | `float` |  |  | CC: Line Discount Amount in Data Warehouse Currency |
| `cocp_trnc` | `float` | `float` |  |  | CC: Company Owned Cost Price in Transaction Currency |
| `cocp_lclc` | `float` | `float` |  |  | CC: Company Owned Cost Price in Local Currency |
| `cocp_rpc1` | `float` | `float` |  |  | CC: Company Owned Cost Price in Reporting Currency 1 |
| `cocp_rpc2` | `float` | `float` |  |  | CC: Company Owned Cost Price in Reporting Currency 2 |
| `cocp_rfrc` | `float` | `float` |  |  | CC: Company Owned Cost Price in Reference Currency |
| `cocp_dtwc` | `float` | `float` |  |  | CC: Company Owned Cost Price in Data Warehouse Currency |
| `cucp_trnc` | `float` | `float` |  |  | CC: Customer Owned Cost Price in Transaction Currency |
| `cucp_lclc` | `float` | `float` |  |  | CC: Customer Owned Cost Price in Local Currency |
| `cucp_rpc1` | `float` | `float` |  |  | CC: Customer Owned Cost Price in Reporting Currency 1 |
| `cucp_rpc2` | `float` | `float` |  |  | CC: Customer Owned Cost Price in Reporting Currency 2 |
| `cucp_rfrc` | `float` | `float` |  |  | CC: Customer Owned Cost Price in Reference Currency |
| `cucp_dtwc` | `float` | `float` |  |  | CC: Customer Owned Cost Price in Data Warehouse Currency |
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

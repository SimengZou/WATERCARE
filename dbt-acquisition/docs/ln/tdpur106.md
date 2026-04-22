# tdpur106

LN tdpur106 RFQ Reponses table, Generated 2026-04-10T19:41:19Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur106` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `qono`, `pono`, `srnb`, `otbp` |
| **Column count** | 314 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `qono` | `string` | `varchar` | `PK` | `not_null` | (required) RFQ. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `srnb` | `integer` | `int` | `PK` | `not_null` | (required) Response Sequence |
| `otbp` | `string` | `varchar` | `PK` | `not_null` | (required) Bidder. Max length: 9 |
| `plin` | `integer` | `int` |  |  | Alternative |
| `pseq` | `integer` | `int` |  |  | Sequence |
| `ltyp` | `integer` | `int` |  |  | Line Type. Allowed values: 5, 10, 15 |
| `ltyp_kw` | `string` | `varchar` |  |  | Line Type (keyword). Allowed values: tcgen.ltyp.total, tcgen.ltyp.detail, tcgen.ltyp.line |
| `kofl` | `integer` | `int` |  |  | Kind of Line. Allowed values: 5, 10, 15 |
| `kofl_kw` | `string` | `varchar` |  |  | Kind of Line (keyword). Allowed values: tcgen.kofl.primary, tcgen.kofl.alternative, tcgen.kofl.not.appl |
| `cofc` | `string` | `varchar` |  |  | Purchase Office. Max length: 6 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `ccon` | `string` | `varchar` |  |  | Buyer. Max length: 9 |
| `exdt` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `sfad` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `sfcn` | `string` | `varchar` |  |  | Ship-from Contact. Max length: 9 |
| `citg` | `string` | `varchar` |  |  | Item Group. Max length: 6 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `effn` | `integer` | `int` |  |  | Effectivity Unit |
| `citt` | `string` | `varchar` |  |  | Item Code System. Max length: 3 |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `mpnr` | `string` | `varchar` |  |  | Manufacturer Part Number. Max length: 35 |
| `cmnf` | `string` | `varchar` |  |  | Manufacturer. Max length: 6 |
| `revi` | `string` | `varchar` |  |  | Engineering Item Revision. Max length: 6 |
| `acqm` | `integer` | `int` |  |  | Acquisition Method. Allowed values: 5, 10, 99 |
| `acqm_kw` | `string` | `varchar` |  |  | Acquisition Method (keyword). Allowed values: tcacqm.buying, tcacqm.rental, tcacqm.not.appl |
| `qimf` | `float` | `float` |  |  | Order Quantity Increment |
| `qimi` | `float` | `float` |  |  | Minimum Order Quantity |
| `qoor` | `float` | `float` |  |  | Quantity |
| `cuqp` | `string` | `varchar` |  |  | Purchase Unit. Max length: 3 |
| `cvqp` | `float` | `float` |  |  | Purchase Unit Conversion Factor |
| `pric` | `float` | `float` |  |  | Price |
| `cupp` | `string` | `varchar` |  |  | Price Unit. Max length: 3 |
| `cvpp` | `float` | `float` |  |  | Price Unit Conversion Factor |
| `porg` | `integer` | `int` |  |  | Price Origin. Allowed values: 2, 4, 6, 8, 10, 12, 13, 14, 16, 18, 20, 25, 30, 35, 40 |
| `porg_kw` | `string` | `varchar` |  |  | Price Origin (keyword). Allowed values: tdgen.porg.not.applicable, tdgen.porg.manual, tdgen.porg.contract, tdgen.porg.variant, tdgen.porg.item.pur, tdgen.porg.item.sls, tdgen.porg.item.service, tdgen.porg.supp.pr.book, tdgen.porg.def.pr.book, tdgen.porg.price.structure, tdgen.porg.extern, tdgen.porg.consumption, tdgen.porg.generic.pr.list, tdgen.porg.project.budget, tdgen.porg.projectcost.obj |
| `pmde` | `string` | `varchar` |  |  | Price Matrix Definition. Max length: 9 |
| `pmse` | `integer` | `int` |  |  | Price Matrix Sequence |
| `prbk` | `string` | `varchar` |  |  | Price Book. Max length: 9 |
| `prsg` | `string` | `varchar` |  |  | Price Stage. Max length: 3 |
| `disc_1` | `float` | `float` |  |  | Discount % |
| `disc_2` | `float` | `float` |  |  | Discount % |
| `disc_3` | `float` | `float` |  |  | Discount % |
| `disc_4` | `float` | `float` |  |  | Discount % |
| `disc_5` | `float` | `float` |  |  | Discount % |
| `disc_6` | `float` | `float` |  |  | Discount % |
| `disc_7` | `float` | `float` |  |  | Discount % |
| `disc_8` | `float` | `float` |  |  | Discount % |
| `disc_9` | `float` | `float` |  |  | Discount % |
| `disc_10` | `float` | `float` |  |  | Discount % |
| `disc_11` | `float` | `float` |  |  | Discount % |
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
| `dmde_1` | `string` | `varchar` |  |  | Discount Matrix Definition. Max length: 9 |
| `dmde_2` | `string` | `varchar` |  |  | Discount Matrix Definition. Max length: 9 |
| `dmde_3` | `string` | `varchar` |  |  | Discount Matrix Definition. Max length: 9 |
| `dmde_4` | `string` | `varchar` |  |  | Discount Matrix Definition. Max length: 9 |
| `dmde_5` | `string` | `varchar` |  |  | Discount Matrix Definition. Max length: 9 |
| `dmde_6` | `string` | `varchar` |  |  | Discount Matrix Definition. Max length: 9 |
| `dmde_7` | `string` | `varchar` |  |  | Discount Matrix Definition. Max length: 9 |
| `dmde_8` | `string` | `varchar` |  |  | Discount Matrix Definition. Max length: 9 |
| `dmde_9` | `string` | `varchar` |  |  | Discount Matrix Definition. Max length: 9 |
| `dmde_10` | `string` | `varchar` |  |  | Discount Matrix Definition. Max length: 9 |
| `dmde_11` | `string` | `varchar` |  |  | Discount Matrix Definition. Max length: 9 |
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
| `dssc_1` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_2` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_3` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_4` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_5` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_6` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_7` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_8` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_9` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_10` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
| `dssc_11` | `string` | `varchar` |  |  | Discount Schedule. Max length: 9 |
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
| `amta` | `float` | `float` |  |  | Amount |
| `qspa` | `integer` | `int` |  |  | Status. Allowed values: 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60 |
| `qspa_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tdpur.rfqs.created, tdpur.rfqs.pending.resp, tdpur.rfqs.sent, tdpur.rfqs.modified, tdpur.rfqs.responded, tdpur.rfqs.in.process, tdpur.rfqs.no.bid, tdpur.rfqs.no.response, tdpur.rfqs.negotiating, tdpur.rfqs.accepted, tdpur.rfqs.rejected, tdpur.rfqs.processed, tdpur.rfqs.cancelled |
| `cnat` | `integer` | `int` |  |  | Conversion Action. Allowed values: 5, 10, 20, 30, 100 |
| `cnat_kw` | `string` | `varchar` |  |  | Conversion Action (keyword). Allowed values: tdpur.actn.pending, tdpur.actn.convert, tdpur.actn.ignore, tdpur.actn.delete, tdpur.actn.not.appl |
| `cnty` | `integer` | `int` |  |  | Conversion Type. Allowed values: 10, 20, 30, 40, 50, 100 |
| `cnty_kw` | `string` | `varchar` |  |  | Conversion Type (keyword). Allowed values: tdpur.cnvs.order, tdpur.cnvs.contract, tdpur.cnvs.pricebook, tdpur.cnvs.order.contract, tdpur.cnvs.orderpricebook, tdpur.cnvs.not.appl |
| `prih_1` | `float` | `float` |  |  | Price in Home Currency |
| `prih_2` | `float` | `float` |  |  | Price in Home Currency |
| `prih_3` | `float` | `float` |  |  | Price in Home Currency |
| `send` | `integer` | `int` |  |  | Send RFQ. Allowed values: 1, 2 |
| `send_kw` | `string` | `varchar` |  |  | Send RFQ (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `qdat` | `timestamp` | `timestamp_ntz` |  |  | RFQ Date |
| `rtdt` | `timestamp` | `timestamp_ntz` |  |  | Response Date |
| `ddat` | `timestamp` | `timestamp_ntz` |  |  | Receipt Date |
| `sdat` | `timestamp` | `timestamp_ntz` |  |  | Start Date for Receipt Period |
| `edat` | `timestamp` | `timestamp_ntz` |  |  | End Date for Receipt Period |
| `lead` | `integer` | `int` |  |  | Lead Time |
| `leun` | `integer` | `int` |  |  | Lead Time Unit. Allowed values: 5, 10 |
| `leun_kw` | `string` | `varchar` |  |  | Lead Time Unit (keyword). Allowed values: tctope.hours, tctope.days |
| `suti` | `float` | `float` |  |  | Supply Time |
| `sutu` | `integer` | `int` |  |  | Unit for Supply Time. Allowed values: 5, 10 |
| `sutu_kw` | `string` | `varchar` |  |  | Unit for Supply Time (keyword). Allowed values: tctope.hours, tctope.days |
| `sdsc` | `integer` | `int` |  |  | Standard Description. Allowed values: 1, 2 |
| `sdsc_kw` | `string` | `varchar` |  |  | Standard Description (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `leng` | `float` | `float` |  |  | Length |
| `widt` | `float` | `float` |  |  | Width |
| `thic` | `float` | `float` |  |  | Height |
| `lcmp` | `integer` | `int` |  |  | Logistic Company |
| `rcsi` | `string` | `varchar` |  |  | Receiving Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `cadr` | `string` | `varchar` |  |  | Receipt Address. Max length: 9 |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `ccty` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `ceno` | `string` | `varchar` |  |  | Exempt Certificate. Max length: 24 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `rcod` | `string` | `varchar` |  |  | Exempt Reason. Max length: 6 |
| `bptc` | `string` | `varchar` |  |  | Business Partner Tax Country. Max length: 3 |
| `exmt` | `integer` | `int` |  |  | Exempt. Allowed values: 1, 2 |
| `exmt_kw` | `string` | `varchar` |  |  | Exempt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `gefo` | `integer` | `int` |  |  | Generate Freight Orders from Purchase. Allowed values: 1, 2 |
| `gefo_kw` | `string` | `varchar` |  |  | Generate Freight Orders from Purchase (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cfrw` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `crbn` | `integer` | `int` |  |  | Carrier Binding. Allowed values: 1, 2 |
| `crbn_kw` | `string` | `varchar` |  |  | Carrier Binding (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `serv` | `string` | `varchar` |  |  | Freight Service Level. Max length: 3 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `pegd` | `integer` | `int` |  |  | Peg Distribution. Allowed values: 1, 2 |
| `pegd_kw` | `string` | `varchar` |  |  | Peg Distribution (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `issp` | `integer` | `int` |  |  | Invoice by Stage Payments. Allowed values: 1, 2 |
| `issp_kw` | `string` | `varchar` |  |  | Invoice by Stage Payments (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lccl` | `string` | `varchar` |  |  | Landed Costs Classification. Max length: 12 |
| `ipla` | `integer` | `int` |  |  | Include in Planning. Allowed values: 1, 2 |
| `ipla_kw` | `string` | `varchar` |  |  | Include in Planning (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mfdt` | `timestamp` | `timestamp_ntz` |  |  | Manufacture Date |
| `lbuy` | `integer` | `int` |  |  | Last Time Buy. Allowed values: 1, 2 |
| `lbuy_kw` | `string` | `varchar` |  |  | Last Time Buy (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lbdt` | `timestamp` | `timestamp_ntz` |  |  | Last Time Buy Date |
| `warr` | `integer` | `int` |  |  | Warranty. Allowed values: 1, 2 |
| `warr_kw` | `string` | `varchar` |  |  | Warranty (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `eolf` | `integer` | `int` |  |  | End of Life. Allowed values: 1, 2 |
| `eolf_kw` | `string` | `varchar` |  |  | End of Life (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `etpc` | `integer` | `int` |  |  | Exclude from Target Price Calculation. Allowed values: 1, 2 |
| `etpc_kw` | `string` | `varchar` |  |  | Exclude from Target Price Calculation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tpbk` | `string` | `varchar` |  |  | Target Price Book. Max length: 9 |
| `tpbl` | `integer` | `int` |  |  | Target Price Book Line |
| `tapr` | `float` | `float` |  |  | Target Price |
| `ares` | `integer` | `int` |  |  | Additional Response. Allowed values: 1, 2 |
| `ares_kw` | `string` | `varchar` |  |  | Additional Response (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crcd` | `string` | `varchar` |  |  | Change Reason. Max length: 6 |
| `ctcd` | `string` | `varchar` |  |  | Change Type. Max length: 6 |
| `adin` | `string` | `varchar` |  |  | Additional Information. Max length: 22 |
| `note` | `string` | `varchar` |  |  | Note GUID. Max length: 22 |
| `pbor` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2, 3, 4 |
| `pbor_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tdpcg.pbor.stnd, tdpcg.pbor.socn, tdpcg.pbor.pocn, tdpcg.pbor.rfqo |
| `dsor` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2, 3, 4, 5 |
| `dsor_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tdpcg.dsor.stnd, tdpcg.dsor.socn, tdpcg.dsor.pocn, tdpcg.dsor.rfqo, tdpcg.dsor.prom |
| `copy` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 1, 2, 3, 4, 5 |
| `copy_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tdpur.copyrfq.no, tdpur.copyrfq.contract, tdpur.copyrfq.order, tdpur.copyrfq.pricebook, tdpur.copyrfq.delete |
| `rank` | `integer` | `int` |  |  | Obsolete |
| `txta` | `integer` | `int` |  |  | RFQ Line Text |
| `txtb` | `integer` | `int` |  |  | Response Text |
| `qono_otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur105 RFQ Bidders |
| `qono_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur100 Requests for Quotation |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur012 Purchase Offices |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `ccon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom121 Ship-from Business Partners |
| `sfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `sfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `citg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs023 Item Groups |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu001 Item - Purchase |
| `effn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcuef002 Effectivity Units |
| `citt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd006 Item Code Systems |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `mpnr_cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdipu045 Manufacturer Part Numbers |
| `cmnf_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs060 Manufacturers |
| `cuqp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cupp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `prbk_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpcg011 Price Books |
| `prsg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs212 Price Stages |
| `lcmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `rcsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `ccty_cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs036 Tax Codes by Country |
| `rcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `bptc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `cfrw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `serv_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs075 Freight Service Levels |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `lccl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tclct020 Landed Costs Classifications |
| `tpbk_tpbl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpcg033 Target Price Book Lines |
| `tpbk_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpcg013 Target Price Books |
| `crcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur097 Change Reasons |
| `ctcd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur098 Change Types |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

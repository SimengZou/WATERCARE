# tdpur430

LN tdpur430 Purchase Payable Receipts for Orders and Schedules table, Generated 2026-04-10T19:41:23Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur430` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `orno`, `pono`, `sqnb`, `rsqn`, `psqn` |
| **Column count** | 189 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `orno` | `string` | `varchar` | `PK` | `not_null` | (required) Purchase Order/Schedule. Max length: 9 |
| `pono` | `integer` | `int` | `PK` | `not_null` | (required) Line Position |
| `sqnb` | `integer` | `int` | `PK` | `not_null` | (required) Line Sequence |
| `rsqn` | `integer` | `int` | `PK` | `not_null` | (required) Receipt Sequence |
| `psqn` | `integer` | `int` | `PK` | `not_null` | (required) Payable Receipt Sequence |
| `styp` | `integer` | `int` |  |  | Schedule Type. Allowed values: 1, 2, 3, 4 |
| `styp_kw` | `string` | `varchar` |  |  | Schedule Type (keyword). Allowed values: tdstyp.push, tdstyp.pull.forecast, tdstyp.pull.calloff, tdstyp.not.applicable |
| `ptyp` | `integer` | `int` |  |  | Payable Receipt Type. Allowed values: 1, 2, 3 |
| `ptyp_kw` | `string` | `varchar` |  |  | Payable Receipt Type (keyword). Allowed values: tdpur.ptyp.order, tdpur.ptyp.push, tdpur.ptyp.pull |
| `usdt` | `timestamp` | `timestamp_ntz` |  |  | Usage Date |
| `uldt` | `timestamp` | `timestamp_ntz` |  |  | Usage Log Date |
| `qipr` | `float` | `float` |  |  | Payable Quantity in Inventory Unit |
| `qppr` | `float` | `float` |  |  | Payable Quantity in Piece Unit |
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
| `stsc` | `integer` | `int` |  |  | Update Status of Actual Purchase Costs. Allowed values: 1, 2 |
| `stsc_kw` | `string` | `varchar` |  |  | Update Status of Actual Purchase Costs (keyword). Allowed values: tcstsc.free, tcstsc.updated |
| `stsd` | `integer` | `int` |  |  | Invoicing Status. Allowed values: 1, 2, 3, 4 |
| `stsd_kw` | `string` | `varchar` |  |  | Invoicing Status (keyword). Allowed values: tcstsd.free, tcstsd.approved, tcstsd.all.approved, tcstsd.matched |
| `qiiv` | `float` | `float` |  |  | Invoiced Quantity |
| `iamt` | `float` | `float` |  |  | Invoiced Amount |
| `qpiv` | `float` | `float` |  |  | Invoiced Quantity in Piece Unit |
| `invn` | `string` | `varchar` |  |  | Invoice Number. Max length: 9 |
| `invd` | `timestamp` | `timestamp_ntz` |  |  | Invoice Date |
| `cuva` | `float` | `float` |  |  | Customs Value |
| `cuvc` | `string` | `varchar` |  |  | Customs Value Currency. Max length: 3 |
| `cono` | `string` | `varchar` |  |  | Contract. Max length: 9 |
| `cpon` | `integer` | `int` |  |  | Contract Position |
| `ccof` | `string` | `varchar` |  |  | Contract Purchase Office. Max length: 6 |
| `csqn` | `integer` | `int` |  |  | Contract Sequence |
| `cnig` | `integer` | `int` |  |  | Contract Ignored. Allowed values: 1, 2 |
| `cnig_kw` | `string` | `varchar` |  |  | Contract Ignored (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pamt` | `float` | `float` |  |  | Payable Amount in Order Currency |
| `amod` | `float` | `float` |  |  | Order Discount Amount |
| `amld` | `float` | `float` |  |  | Order Line Discount Amount |
| `rarc` | `integer` | `int` |  |  | Reason for Amount Change. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8 |
| `rarc_kw` | `string` | `varchar` |  |  | Reason for Amount Change (keyword). Allowed values: tdpur.rach.receipt, tdpur.rach.rec.corr, tdpur.rach.inspection, tdpur.rach.issue, tdpur.rach.pric.change, tdpur.rach.not.appl, tdpur.rach.consumption, tdpur.rach.proc.pur.order |
| `ract` | `integer` | `int` |  |  | Retro-Active. Allowed values: 1, 2 |
| `ract_kw` | `string` | `varchar` |  |  | Retro-Active (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lssn` | `string` | `varchar` |  |  | Item Identification Set. Max length: 22 |
| `opsq` | `integer` | `int` |  |  | Original Payable |
| `ftdt` | `timestamp` | `timestamp_ntz` |  |  | Finance Transaction Date |
| `proc` | `integer` | `int` |  |  | Processed. Allowed values: 1, 2 |
| `proc_kw` | `string` | `varchar` |  |  | Processed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prsg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs212 Price Stages |
| `cupp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cuvc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `lssn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd410 Item Identification Sets |
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

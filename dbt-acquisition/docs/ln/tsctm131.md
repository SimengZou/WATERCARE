# tsctm131

LN tsctm131 Material Terms table, Generated 2026-04-10T19:42:31Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsctm131` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `term`, `cfln`, `cctp`, `cotp`, `nrbt`, `cseq` |
| **Column count** | 242 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `term` | `integer` | `int` | `PK` | `not_null` | (required) TermID |
| `cfln` | `integer` | `int` | `PK` | `not_null` | (required) Configuration Line |
| `cctp` | `string` | `varchar` | `PK` | `not_null` | (required) Coverage Type. Max length: 3 |
| `cotp` | `integer` | `int` | `PK` | `not_null` | (required) Term Type. Allowed values: 5, 10, 15, 20, 25, 30, 35, 40, 45 |
| `cotp_kw` | `string` | `varchar` |  |  | Term Type (keyword). Allowed values: tsctm.tmtp.material, tsctm.tmtp.labor, tsctm.tmtp.tool, tsctm.tmtp.travel, tsctm.tmtp.subcon, tsctm.tmtp.helpdesk, tsctm.tmtp.other, tsctm.tmtp.uptime, tsctm.tmtp.all |
| `nrbt` | `integer` | `int` | `PK` | `not_null` | (required) Coverage Line |
| `cseq` | `integer` | `int` | `PK` | `not_null` | (required) Material Line |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `csgr` | `string` | `varchar` |  |  | Service Item Group. Max length: 6 |
| `ccmp` | `string` | `varchar` |  |  | Cost Component. Max length: 8 |
| `quan` | `float` | `float` |  |  | Quantity |
| `pric_1` | `float` | `float` |  |  | Unit Cost |
| `pric_2` | `float` | `float` |  |  | Unit Cost |
| `pric_3` | `float` | `float` |  |  | Unit Cost |
| `pris` | `float` | `float` |  |  | Sales Price |
| `camt_1` | `float` | `float` |  |  | Cost Amount |
| `camt_2` | `float` | `float` |  |  | Cost Amount |
| `camt_3` | `float` | `float` |  |  | Cost Amount |
| `amnt` | `float` | `float` |  |  | Sales Amount |
| `porg` | `integer` | `int` |  |  | Price Origin. Allowed values: 5, 10, 15, 20, 25, 30, 35, 45, 50, 55, 60, 99 |
| `porg_kw` | `string` | `varchar` |  |  | Price Origin (keyword). Allowed values: tstdm.porg.manual, tstdm.porg.other, tstdm.porg.serv.contract, tstdm.porg.master.routing, tstdm.porg.routing.option, tstdm.porg.ref.activity, tstdm.porg.trav.rate.book, tstdm.porg.item.serv, tstdm.porg.def.pr.book, tstdm.porg.price.structure, tstdm.porg.serial, tstdm.porg.not.applicable |
| `pmde` | `string` | `varchar` |  |  | Price Matrix. Max length: 9 |
| `pmse` | `integer` | `int` |  |  | Price Matrix Sequence |
| `disc_1` | `float` | `float` |  |  | Discount Percentage |
| `disc_2` | `float` | `float` |  |  | Discount Percentage |
| `disc_3` | `float` | `float` |  |  | Discount Percentage |
| `disc_4` | `float` | `float` |  |  | Discount Percentage |
| `disc_5` | `float` | `float` |  |  | Discount Percentage |
| `disc_6` | `float` | `float` |  |  | Discount Percentage |
| `disc_7` | `float` | `float` |  |  | Discount Percentage |
| `disc_8` | `float` | `float` |  |  | Discount Percentage |
| `disc_9` | `float` | `float` |  |  | Discount Percentage |
| `disc_10` | `float` | `float` |  |  | Discount Percentage |
| `disc_11` | `float` | `float` |  |  | Discount Percentage |
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
| `dtrm` | `integer` | `int` |  |  | Determining. Allowed values: 1, 2 |
| `dtrm_kw` | `string` | `varchar` |  |  | Determining (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `elgb` | `integer` | `int` |  |  | Eligible. Allowed values: 1, 2 |
| `elgb_kw` | `string` | `varchar` |  |  | Eligible (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dspc` | `integer` | `int` |  |  | Term is Derived from Reference Activity. Allowed values: 1, 2 |
| `dspc_kw` | `string` | `varchar` |  |  | Term is Derived from Reference Activity (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `spqu` | `float` | `float` |  |  | Spent Quantity |
| `spco_1` | `float` | `float` |  |  | Spent Costs |
| `spco_2` | `float` | `float` |  |  | Spent Costs |
| `spco_3` | `float` | `float` |  |  | Spent Costs |
| `spsa` | `float` | `float` |  |  | Spent Sales |
| `alqu` | `float` | `float` |  |  | Allocated Quantity |
| `alco_1` | `float` | `float` |  |  | Allocated Costs |
| `alco_2` | `float` | `float` |  |  | Allocated Costs |
| `alco_3` | `float` | `float` |  |  | Allocated Costs |
| `alsa` | `float` | `float` |  |  | Allocated Sales |
| `ealc_1` | `float` | `float` |  |  | Estimated Allocated Costs |
| `ealc_2` | `float` | `float` |  |  | Estimated Allocated Costs |
| `ealc_3` | `float` | `float` |  |  | Estimated Allocated Costs |
| `eals` | `float` | `float` |  |  | Estimated Allocated Sales |
| `eaqu` | `float` | `float` |  |  | Estimated Allocated Quantity |
| `tcam` | `float` | `float` |  |  | Total Ceiling Amount |
| `tcqa` | `float` | `float` |  |  | Total Ceiling Quantity |
| `txta` | `integer` | `int` |  |  | Text |
| `term_cfln_cctp_cotp_nrbt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm120 Coverage Terms |
| `term_cfln_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm110 Configuration Lines |
| `cctp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm035 Coverage Types |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm200 Items - Service |
| `csgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm210 Service Item Groups |
| `ccmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs048 Cost Components |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `camt_cntc` | `float` | `float` |  |  | CC: Cost Amount in Contract Currency |
| `camt_refc` | `float` | `float` |  |  | CC: Cost Amount in Reference Currency |
| `camt_dwhc` | `float` | `float` |  |  | CC: Cost Amount in Data Warehouse Currency |
| `amnt_homc` | `float` | `float` |  |  | CC: Sales Amount in Local Currency |
| `amnt_rpc1` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 1 |
| `amnt_rpc2` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 2 |
| `amnt_refc` | `float` | `float` |  |  | CC: Sales Amount in Reference Currency |
| `amnt_dwhc` | `float` | `float` |  |  | CC: Sales Amount in Data Warehouse Currency |
| `spco_cntc` | `float` | `float` |  |  | CC: Spent Cost Amount in Contract Currency |
| `spco_refc` | `float` | `float` |  |  | CC: Spent Cost Amount in Reference Currency |
| `spco_dwhc` | `float` | `float` |  |  | CC: Spent Cost Amount in Data Warehouse Currency |
| `spsa_homc` | `float` | `float` |  |  | CC: Spent Sales Amount in Local Currency |
| `spsa_rpc1` | `float` | `float` |  |  | CC: Spent Sales Amount in Reporting Currency 1 |
| `spsa_rpc2` | `float` | `float` |  |  | CC: Spent Sales Amount in Reporting Currency 2 |
| `spsa_refc` | `float` | `float` |  |  | CC: Spent Sales Amount in Reference Currency |
| `spsa_dwhc` | `float` | `float` |  |  | CC: Spent Sales Amount in Data Warehouse Currency |
| `alco_cntc` | `float` | `float` |  |  | CC: Allocated Cost in Contract Currency |
| `alco_refc` | `float` | `float` |  |  | CC: Allocated Cost Amount in Reference Currency |
| `alco_dwhc` | `float` | `float` |  |  | CC: Allocated Cost Amount in Data Warehouse Currency |
| `alsa_homc` | `float` | `float` |  |  | CC: Allocated Sales Amount in Local Currency |
| `alsa_rpc1` | `float` | `float` |  |  | CC: Allocated Sales Amount in Reporting Currency 1 |
| `alsa_rpc2` | `float` | `float` |  |  | CC: Allocated Sales Amount in Reporting Currency 2 |
| `alsa_refc` | `float` | `float` |  |  | CC: Allocated Sales Amount in Reference Currency |
| `alsa_dwhc` | `float` | `float` |  |  | CC: Allocated Sales Amount in Data Warehouse Currency |
| `ealc_cntc` | `float` | `float` |  |  | CC: Estimated Allocated Cost in Contract Currency |
| `ealc_refc` | `float` | `float` |  |  | CC: Estimated Allocated Cost Amount in Reference Currency |
| `ealc_dwhc` | `float` | `float` |  |  | CC: Estimated Allocated Cost Amount in Data Warehouse Currency |
| `eals_homc` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Local Currency |
| `eals_rpc1` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Reporting Currency 1 |
| `eals_rpc2` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Reporting Currency 2 |
| `eals_refc` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Reference Currency |
| `eals_dwhc` | `float` | `float` |  |  | CC: Estimated Allocated Sales Amount in Data Warehouse Currency |
| `alqu_buar` | `float` | `float` |  |  | CC: Allocated Quantity in Base Unit Area |
| `alqu_buln` | `float` | `float` |  |  | CC: Allocated Quantity in Base Unit Length |
| `alqu_buvl` | `float` | `float` |  |  | CC: Allocated Quantity in Base Unit Volume |
| `alqu_buwg` | `float` | `float` |  |  | CC: Allocated Quantity in Base Unit Weight |
| `alqu_bupc` | `float` | `float` |  |  | CC: Allocated Quantity in Base Unit Piece |
| `alqu_invu` | `float` | `float` |  |  | CC: Allocated Quantity in Inventory Unit |
| `spqu_buar` | `float` | `float` |  |  | CC: Spent Quantity in Base Unit Area |
| `spqu_buln` | `float` | `float` |  |  | CC: Spent Quantity in Base Unit Length |
| `spqu_buvl` | `float` | `float` |  |  | CC: Spent Quantity in Base Unit Volume |
| `spqu_buwg` | `float` | `float` |  |  | CC: Spent Quantity in Base Unit Weight |
| `spqu_bupc` | `float` | `float` |  |  | CC: Spent Quantity in Base Unit Piece |
| `spqu_invu` | `float` | `float` |  |  | CC: Spent Quantity in Inventory Unit |
| `quan_bupc` | `float` | `float` |  |  | CC: Quantity in Base Unit Piece |
| `quan_invu` | `float` | `float` |  |  | CC: Quantity in Inventory Unit |
| `quan_buar` | `float` | `float` |  |  | CC: Quantity in Base Unit Area |
| `quan_buln` | `float` | `float` |  |  | CC: Quantity in Base Unit Length |
| `quan_buvl` | `float` | `float` |  |  | CC: Quantity in Base Unit Volume |
| `quan_buwg` | `float` | `float` |  |  | CC: Quantity in Base Unit Weight |
| `eaqu_buln` | `float` | `float` |  |  | CC: Estimated Allocated Quantity in Base Unit Length |
| `eaqu_butm` | `float` | `float` |  |  | CC: Estimated Allocated Quantity in Base Unit Time |
| `eaqu_buvl` | `float` | `float` |  |  | CC: Estimated Allocated Quantity in Base Unit Volume |
| `eaqu_buwg` | `float` | `float` |  |  | CC: Estimated Allocated Quantity in Base Unit Weight |
| `eaqu_bupc` | `float` | `float` |  |  | CC: Estimated Allocated Quantity in Base Unit Piece |
| `eaqu_invu` | `float` | `float` |  |  | CC: Estimated Allocated Quantity in Inventory Unit |
| `eaqu_buar` | `float` | `float` |  |  | CC: Estimated Allocated Quantity in Base Unit Area |
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

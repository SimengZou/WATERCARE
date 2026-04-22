# tfgld004

LN tfgld004 Financial Companies table, Generated 2026-04-10T19:41:40Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld004` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `indt` |
| **Column count** | 571 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `indt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Effective Date |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `year` | `integer` | `int` |  |  | Current Fiscal Year |
| `gcmp` | `integer` | `int` |  |  | Group Company |
| `budg` | `string` | `varchar` |  |  | Actual Budget. Max length: 3 |
| `hcmp` | `integer` | `int` |  |  | Obsolete |
| `acdp` | `string` | `varchar` |  |  | Accounting Department. Max length: 6 |
| `addr` | `string` | `varchar` |  |  | Declarant's Address. Max length: 9 |
| `btyp` | `integer` | `int` |  |  | Default Access by Batch. Allowed values: 1, 2 |
| `btyp_kw` | `string` | `varchar` |  |  | Default Access by Batch (keyword). Allowed values: tfgld.btyp.multi.user, tfgld.btyp.single.user |
| `tbat` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `tbat_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tfgld.tbat.multi.user, tfgld.tbat.single.user |
| `jprt` | `integer` | `int` |  |  | Journal Print With Finalization. Allowed values: 1, 2 |
| `jprt_kw` | `string` | `varchar` |  |  | Journal Print With Finalization (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sprt` | `integer` | `int` |  |  | Status Report with Finalization. Allowed values: 0, 1, 2 |
| `sprt_kw` | `string` | `varchar` |  |  | Status Report with Finalization (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `plat` | `integer` | `int` |  |  | Print Ledger Account Totals Report. Allowed values: 1, 2 |
| `plat_kw` | `string` | `varchar` |  |  | Print Ledger Account Totals Report (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdif` | `integer` | `int` |  |  | Ledger Accounts Currency Differences by Currency. Allowed values: 1, 2 |
| `cdif_kw` | `string` | `varchar` |  |  | Ledger Accounts Currency Differences by Currency (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `clac` | `string` | `varchar` |  |  | Currency Difference Profit Account. Max length: 12 |
| `cla1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `cla2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `cla3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `cla4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `cla5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `cla6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `cla7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `cla8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `cla9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `ca10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `ca11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `ca12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `clcl` | `string` | `varchar` |  |  | Currency Difference Loss Account. Max length: 12 |
| `clc1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `clc2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `clc3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `clc4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `clc5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `clc6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `clc7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `clc8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `clc9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `cc10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `cc11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `cc12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `ctyp` | `string` | `varchar` |  |  | Currency Diff. Transaction Type. Max length: 3 |
| `fpor` | `string` | `varchar` |  |  | Interim AC Fisc. Period Change - Original. Max length: 12 |
| `fpnw` | `string` | `varchar` |  |  | Interim AC Fisc. Period Change - New. Max length: 12 |
| `rpor` | `string` | `varchar` |  |  | Interim A/C Rep. Period Change - Original. Max length: 12 |
| `rpnw` | `string` | `varchar` |  |  | Interim A/C Rep. Period Change - New. Max length: 12 |
| `ilac` | `string` | `varchar` |  |  | Retained Earnings Account Statutory. Max length: 12 |
| `rsd1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `rsd2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `rsd3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `rsd4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `rsd5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `rsd6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `rsd7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `rsd8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `rsd9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `rs10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `rs11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `rs12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `inac` | `string` | `varchar` |  |  | Currency Translation Account. Max length: 12 |
| `incc` | `string` | `varchar` |  |  | Currency Translation Account Complementary. Max length: 12 |
| `ind1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `ind2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `ind3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `ind4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `ind5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `ind6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `ind7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `ind8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `ind9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `id10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `id11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `id12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `inc1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `inc2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `inc3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `inc4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `inc5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `inc6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `inc7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `inc8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `inc9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `ic10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `ic11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `ic12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `roda` | `string` | `varchar` |  |  | Rounding Difference Account  Statutory. Max length: 12 |
| `rod1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `rod2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `rod3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `rod4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `rod5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `rod6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `rod7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `rod8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `rod9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `rd10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `rd11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `rd12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `blpl` | `integer` | `int` |  |  | Balancing of Profit & Loss. Allowed values: 1, 2, 3, 4 |
| `blpl_kw` | `string` | `varchar` |  |  | Balancing of Profit & Loss (keyword). Allowed values: tfgld.blpl.individual, tfgld.blpl.subtotal, tfgld.blpl.earnings, tfgld.blpl.indiv.close.bal |
| `blac` | `string` | `varchar` |  |  | Balancing Account Profit &  Loss Statutory. Max length: 12 |
| `bld1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `bld2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `bld3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `bld4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `bld5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `bld6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `bld7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `bld8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `bld9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `bd10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `bd11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `bd12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `typ1` | `integer` | `int` |  |  | Dimension Type 1 |
| `typ2` | `integer` | `int` |  |  | Dimension Type 2 |
| `typ3` | `integer` | `int` |  |  | Dimension Type 3 |
| `typ4` | `integer` | `int` |  |  | Dimension Type 4 |
| `typ5` | `integer` | `int` |  |  | Dimension Type 5 |
| `typ6` | `integer` | `int` |  |  | Dimension Type 6 |
| `typ7` | `integer` | `int` |  |  | Dimension Type 7 |
| `typ8` | `integer` | `int` |  |  | Dimension Type 8 |
| `typ9` | `integer` | `int` |  |  | Dimension Type 9 |
| `tp10` | `integer` | `int` |  |  | Dimension Type 10 |
| `tp11` | `integer` | `int` |  |  | Dimension Type 11 |
| `tp12` | `integer` | `int` |  |  | Dimension Type 12 |
| `dga1` | `string` | `varchar` |  |  | Destination Gain Account 1. Max length: 12 |
| `dga2` | `string` | `varchar` |  |  | Destination Gain Account 21. Max length: 12 |
| `dla1` | `string` | `varchar` |  |  | Destination Loss Account 1. Max length: 12 |
| `dla2` | `string` | `varchar` |  |  | Destination Loss Account 2. Max length: 12 |
| `bref` | `integer` | `int` |  |  | Bank Reference Check. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9 |
| `bref_kw` | `string` | `varchar` |  |  | Bank Reference Check (keyword). Allowed values: tfgld.bref.10.check, tfgld.bref.11.check, tfgld.bref.no.check, tfgld.bref.finnish.check, tfgld.bref.not.applicable, tfgld.bref.modulo.10.check, tfgld.bref.10.or.11.check, tfgld.bref.strd.ref.check, tfgld.bref.default |
| `glty` | `string` | `varchar` |  |  | Gain/Loss Transaction Type. Max length: 3 |
| `mcfa` | `string` | `varchar` |  |  | Interim Stat/Local AC Fisc. Period Change-Original. Max length: 12 |
| `mcfb` | `string` | `varchar` |  |  | Interim Stat/Local AC Fisc. Period Change-New. Max length: 12 |
| `mcra` | `string` | `varchar` |  |  | Interim Stat/Local A/C Report Period Change-Original. Max length: 12 |
| `mcrb` | `string` | `varchar` |  |  | Interim Stat/Local A/C Report Period Change-New. Max length: 12 |
| `mcfo` | `string` | `varchar` |  |  | Interim Compl/Both A/C Fiscal Period Change-Original. Max length: 12 |
| `mcfn` | `string` | `varchar` |  |  | Interim Compl/Both AC Fisc. Period Change-New. Max length: 12 |
| `mcro` | `string` | `varchar` |  |  | Interim Compl/Both A/C Report Period Change-Original. Max length: 12 |
| `mcrn` | `string` | `varchar` |  |  | Interim Compl/Both A/C Report Period Change-New. Max length: 12 |
| `mcfc` | `string` | `varchar` |  |  | Interim Compl/Rep AC Fisc. Period Change-Original. Max length: 12 |
| `mcfd` | `string` | `varchar` |  |  | Interim Compl/Rep AC Fisc. Period Change-New. Max length: 12 |
| `mcrc` | `string` | `varchar` |  |  | Interim Compl/Rep A/C Report Period Change-Original. Max length: 12 |
| `mcrd` | `string` | `varchar` |  |  | Interim Compl/Rep A/C Report Period Change-New. Max length: 12 |
| `roca` | `string` | `varchar` |  |  | Rounding Difference Account Complementary. Max length: 12 |
| `roc1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `roc2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `roc3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `roc4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `roc5` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `roc6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `roc7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `roc8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `roc9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `ro10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `ro11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `ro12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `blca` | `string` | `varchar` |  |  | Balancing Account Profit &amp; Loss Complementary. Max length: 12 |
| `blc1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `blc2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `blc3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `blc4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `blc5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `blc6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `blc7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `blc8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `blc9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `bc10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `bc11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `bc12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `ilcc` | `string` | `varchar` |  |  | Retained Earnings Account Complementary. Max length: 12 |
| `rcd1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `rcd2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `rcd3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `rcd4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `rcd5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `rcd6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `rcd7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `rcd8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `rcd9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `rc10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `rc11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `rc12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `smac` | `string` | `varchar` |  |  | Statutory Matching Account. Max length: 12 |
| `cmac` | `string` | `varchar` |  |  | Complementary Matching Account. Max length: 12 |
| `mtol` | `float` | `float` |  |  | Matching Tolerance (Percentage) |
| `atol` | `float` | `float` |  |  | Absolute Tolerance Amount |
| `mtyp` | `string` | `varchar` |  |  | Matching Transaction Type. Max length: 3 |
| `mpie` | `integer` | `int` |  |  | Prevent Incomplete Matching for the same Transaction Currencies. Allowed values: 1, 2 |
| `mpie_kw` | `string` | `varchar` |  |  | Prevent Incomplete Matching for the same Transaction Currencies (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `made` | `integer` | `int` |  |  | Allow Matching Based on Difference for the same Transaction Currencies. Allowed values: 1, 2 |
| `made_kw` | `string` | `varchar` |  |  | Allow Matching Based on Difference for the same Transaction Currencies (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mauc` | `integer` | `int` |  |  | Allow Matching for different Transaction Currencies. Allowed values: 1, 2 |
| `mauc_kw` | `string` | `varchar` |  |  | Allow Matching for different Transaction Currencies (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mpiu` | `integer` | `int` |  |  | Prevent Incomplete Matching for different Transaction Currencies. Allowed values: 1, 2 |
| `mpiu_kw` | `string` | `varchar` |  |  | Prevent Incomplete Matching for different Transaction Currencies (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `madu` | `integer` | `int` |  |  | Allow Matching Based on Difference for different Trans. Currencies. Allowed values: 1, 2 |
| `madu_kw` | `string` | `varchar` |  |  | Allow Matching Based on Difference for different Trans. Currencies (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mucu` | `string` | `varchar` |  |  | Matching Currency for different Transaction Currency. Max length: 3 |
| `srac` | `integer` | `int` |  |  | Dimension Accounting on Retained Earnings. Allowed values: 0, 1, 2 |
| `srac_kw` | `string` | `varchar` |  |  | Dimension Accounting on Retained Earnings (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `taut` | `string` | `varchar` |  |  | Tran. Type for Auto Balancing. Max length: 3 |
| `infa` | `integer` | `int` |  |  | Factor Invoices. Allowed values: 0, 1, 2 |
| `infa_kw` | `string` | `varchar` |  |  | Factor Invoices (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `glac` | `integer` | `int` |  |  | Gain/Loss Account by Currency. Allowed values: 1, 2 |
| `glac_kw` | `string` | `varchar` |  |  | Gain/Loss Account by Currency (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dgla` | `string` | `varchar` |  |  | Statutory Exchange Gain/Loss Account. Max length: 12 |
| `dst1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `dst2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `dst3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `dst4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `dst5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `dst6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `dst7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `dst8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `dst9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `ds10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `ds11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `ds12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `dglc` | `string` | `varchar` |  |  | Complementary Exchange Gain/Loss Account. Max length: 12 |
| `dcm1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `dcm2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `dcm3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `dcm4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `dcm5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `dcm6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `dcm7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `dcm8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `dcm9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `dc10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `dc11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `dc12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `jrbk` | `integer` | `int` |  |  | Journal Book. Allowed values: 0, 1, 2 |
| `jrbk_kw` | `string` | `varchar` |  |  | Journal Book (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `vabk` | `integer` | `int` |  |  | VAT Book. Allowed values: 1, 2 |
| `vabk_kw` | `string` | `varchar` |  |  | VAT Book (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sapi` | `integer` | `int` |  |  | Separate Account for Paid Advance Installments. Allowed values: 1, 2 |
| `sapi_kw` | `string` | `varchar` |  |  | Separate Account for Paid Advance Installments (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `schm` | `integer` | `int` |  |  | Schedules Mandatory. Allowed values: 1, 2 |
| `schm_kw` | `string` | `varchar` |  |  | Schedules Mandatory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `paym` | `integer` | `int` |  |  | Payment Method Mandatory. Allowed values: 1, 2 |
| `paym_kw` | `string` | `varchar` |  |  | Payment Method Mandatory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ause` | `string` | `varchar` |  |  | Authorized User for External Journals. Max length: 16 |
| `etyp` | `string` | `varchar` |  |  | Default Transaction Type for External Journals. Max length: 3 |
| `esrn` | `integer` | `int` |  |  | Default Series for External Journals |
| `ttcj` | `string` | `varchar` |  |  | Transaction Type for Transaction Currency Journals. Max length: 3 |
| `tmfc` | `string` | `varchar` |  |  | Transaction Type for Multi-Functional Currencies Journals. Max length: 3 |
| `badm` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `badm_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `usac` | `integer` | `int` |  |  | Use Suspense Account. Allowed values: 1, 2 |
| `usac_kw` | `string` | `varchar` |  |  | Use Suspense Account (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `spac` | `string` | `varchar` |  |  | Suspense Account. Max length: 12 |
| `acmn` | `integer` | `int` |  |  | New Account Matching Functionality Active. Allowed values: 1, 2 |
| `acmn_kw` | `string` | `varchar` |  |  | New Account Matching Functionality Active (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `amfn` | `integer` | `int` |  |  | Account Matching. Allowed values: 1, 2 |
| `amfn_kw` | `string` | `varchar` |  |  | Account Matching (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fbya` | `integer` | `int` |  |  | Finalization by Administrator. Allowed values: 1, 2 |
| `fbya_kw` | `string` | `varchar` |  |  | Finalization by Administrator (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rjic` | `integer` | `int` |  |  | Recurring Journal Invoice Creation. Allowed values: 1, 2 |
| `rjic_kw` | `string` | `varchar` |  |  | Recurring Journal Invoice Creation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rvtt` | `string` | `varchar` |  |  | Reversal Transaction Type. Max length: 3 |
| `rvse` | `integer` | `int` |  |  | Reversal Series |
| `indm` | `integer` | `int` |  |  | Copy Interperiod Dimensions from Original Transactions. Allowed values: 1, 2 |
| `indm_kw` | `string` | `varchar` |  |  | Copy Interperiod Dimensions from Original Transactions (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crvr` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `crvr_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `isgm` | `string` | `varchar` |  |  | Intersegment Account Statutory. Max length: 12 |
| `isgc` | `string` | `varchar` |  |  | Intersegment Account Complementary. Max length: 12 |
| `sbct` | `string` | `varchar` |  |  | Clearing/Correction Transaction Type. Max length: 3 |
| `bsjv` | `integer` | `int` |  |  | Block Segmented Unbalanced Journal Voucher. Allowed values: 1, 2 |
| `bsjv_kw` | `string` | `varchar` |  |  | Block Segmented Unbalanced Journal Voucher (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cfac` | `string` | `varchar` |  |  | Currency Difference FIFO Profit Account. Max length: 12 |
| `cfa1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `cfa2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `cfa3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `cfa4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `cfa5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `cfa6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `cfa7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `cfa8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `cfa9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `fa10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `fa11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `fa12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `cfcl` | `string` | `varchar` |  |  | Currency Difference FIFO Loss Account. Max length: 12 |
| `cfc1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `cfc2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `cfc3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `cfc4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `cfc5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `cfc6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `cfc7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `cfc8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `cfc9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `fc10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `fc11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `fc12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `cftp` | `string` | `varchar` |  |  | Currency Difference FIFO Transaction Type. Max length: 3 |
| `pfdm` | `integer` | `int` |  |  | Pre-fill Dimensions. Allowed values: 1, 2 |
| `pfdm_kw` | `string` | `varchar` |  |  | Pre-fill Dimensions (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `obst` | `string` | `varchar` |  |  | Opening Balance Statutory. Max length: 12 |
| `obcp` | `string` | `varchar` |  |  | Opening Balance Complementary. Max length: 12 |
| `rest` | `string` | `varchar` |  |  | Closing Balance Statutory. Max length: 12 |
| `recp` | `string` | `varchar` |  |  | Closing Balance Complementary. Max length: 12 |
| `nftr` | `integer` | `int` |  |  | Calculate Currency Difference if Non-Finalized Transactions exist. Allowed values: 1, 2 |
| `nftr_kw` | `string` | `varchar` |  |  | Calculate Currency Difference if Non-Finalized Transactions exist (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `latg` | `integer` | `int` |  |  | Ledger Accounts by Target Account Group. Allowed values: 1, 2 |
| `latg_kw` | `string` | `varchar` |  |  | Ledger Accounts by Target Account Group (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rcac` | `integer` | `int` |  |  | Report Contra Accounts. Allowed values: 1, 2 |
| `rcac_kw` | `string` | `varchar` |  |  | Report Contra Accounts (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rcbp` | `integer` | `int` |  |  | Replace AP/AR Control Account with Business Partner. Allowed values: 0, 1, 2 |
| `rcbp_kw` | `string` | `varchar` |  |  | Replace AP/AR Control Account with Business Partner (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `drpf` | `integer` | `int` |  |  | Descriptive References for Purchase and Freight Cost Lines. Allowed values: 1, 2 |
| `drpf_kw` | `string` | `varchar` |  |  | Descriptive References for Purchase and Freight Cost Lines (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dfdm` | `integer` | `int` |  |  | Default Dimension. Allowed values: 1, 2 |
| `dfdm_kw` | `string` | `varchar` |  |  | Default Dimension (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dssq_1` | `integer` | `int` |  |  | Sequences. Allowed values: 10, 20, 30, 40, 255 |
| `dssq_kw_1` | `string` | `varchar` |  |  | Sequences (keyword). Allowed values: tfgld.dfds.dimn.leac, tfgld.dfds.dimn, tfgld.dfds.bpid.leac, tfgld.dfds.user.leac, tfgld.dfds.not.appl |
| `dssq_2` | `integer` | `int` |  |  | Sequences. Allowed values: 10, 20, 30, 40, 255 |
| `dssq_kw_2` | `string` | `varchar` |  |  | Sequences (keyword). Allowed values: tfgld.dfds.dimn.leac, tfgld.dfds.dimn, tfgld.dfds.bpid.leac, tfgld.dfds.user.leac, tfgld.dfds.not.appl |
| `dssq_3` | `integer` | `int` |  |  | Sequences. Allowed values: 10, 20, 30, 40, 255 |
| `dssq_kw_3` | `string` | `varchar` |  |  | Sequences (keyword). Allowed values: tfgld.dfds.dimn.leac, tfgld.dfds.dimn, tfgld.dfds.bpid.leac, tfgld.dfds.user.leac, tfgld.dfds.not.appl |
| `dssq_4` | `integer` | `int` |  |  | Sequences. Allowed values: 10, 20, 30, 40, 255 |
| `dssq_kw_4` | `string` | `varchar` |  |  | Sequences (keyword). Allowed values: tfgld.dfds.dimn.leac, tfgld.dfds.dimn, tfgld.dfds.bpid.leac, tfgld.dfds.user.leac, tfgld.dfds.not.appl |
| `cdf_pryp` | `string` | `varchar` |  |  | Project Rev Transaction Type. Max length: 3 |
| `cdf_ptyp` | `string` | `varchar` |  |  | Project Transaction Type. Max length: 3 |
| `cdf_ttyp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `cdf_typr` | `string` | `varchar` |  |  | Reversal Transaction Type. Max length: 3 |
| `year_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld006 End Dates by Year |
| `addr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `clac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `clcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `ctyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `fpor_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `fpnw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `rpor_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `rpnw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `ilac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `inac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `incc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `roda_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `blac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `typ1_cla1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ1_clc1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ1_ind1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ1_bld1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ1_rod1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ1_roc1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ1_blc1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ1_inc1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ1_rsd1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ1_rcd1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ1_dst1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ1_dcm1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ1_cfa1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ1_cfc1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ2_cla2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ2_clc2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ2_ind2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ2_bld2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ2_rod2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ2_roc2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ2_blc2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ2_inc2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ2_rsd2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ2_rcd2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ2_dst2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ2_dcm2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ2_cfa2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ2_cfc2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ3_cla3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ3_clc3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ3_ind3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ3_bld3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ3_rod3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ3_roc3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ3_blc3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ3_inc3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ3_rsd3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ3_rcd3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ3_dst3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ3_dcm3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ3_cfa3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ3_cfc3_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ4_cla4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ4_clc4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ4_ind4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ4_bld4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ4_rod4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ4_roc4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ4_blc4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ4_inc4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ4_rsd4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ4_rcd4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ4_dst4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ4_dcm4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ4_cfa4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ4_cfc4_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ5_cla5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ5_clc5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ5_ind5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ5_bld5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ5_rod5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ5_roc5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ5_blc5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ5_inc5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ5_rsd5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ5_rcd5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ5_dst5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ5_dcm5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ5_cfa5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ5_cfc5_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ6_cla6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ6_clc6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ6_ind6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ6_bld6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ6_rod6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ6_roc6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ6_blc6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ6_inc6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ6_rsd6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ6_rcd6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ6_dst6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ6_dcm6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ6_cfa6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ6_cfc6_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ7_cla7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ7_clc7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ7_ind7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ7_bld7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ7_rod7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ7_roc7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ7_blc7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ7_inc7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ7_rsd7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ7_rcd7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ7_dst7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ7_dcm7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ7_cfa7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ7_cfc7_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ8_cla8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ8_clc8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ8_ind8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ8_bld8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ8_rod8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ8_roc8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ8_blc8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ8_inc8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ8_rsd8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ8_rcd8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ8_dst8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ8_dcm8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ8_cfa8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ8_cfc8_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ9_cla9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ9_clc9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ9_ind9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ9_bld9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ9_rod9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ9_roc9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ9_blc9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ9_inc9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ9_rsd9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ9_rcd9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ9_dst9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ9_dcm9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ9_cfa9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `typ9_cfc9_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp10_ca10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp10_cc10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp10_id10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp10_bd10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp10_rd10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp10_ro10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp10_bc10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp10_ic10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp10_rs10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp10_rc10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp10_ds10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp10_dc10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp10_fa10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp10_fc10_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp11_ca11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp11_cc11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp11_id11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp11_bd11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp11_rd11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp11_ro11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp11_bc11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp11_ic11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp11_rs11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp11_rc11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp11_ds11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp11_dc11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp11_fa11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp11_fc11_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp12_ca12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp12_cc12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp12_id12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp12_bd12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp12_rd12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp12_ro12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp12_bc12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp12_ic12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp12_rs12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp12_rc12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp12_ds12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp12_dc12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp12_fa12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `tp12_fc12_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld010 Dimensions |
| `dga1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `dga2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `dla1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `dla2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `glty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `mcfa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `mcfb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `mcra_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `mcrb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `mcfo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `mcfn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `mcro_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `mcrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `mcfc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `mcfd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `mcrc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `mcrd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `roca_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `blca_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `ilcc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `smac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `cmac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `mtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `taut_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `dgla_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `dglc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `etyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `ttcj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `tmfc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `spac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `rvtt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `isgm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `isgc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `sbct_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `cfac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `cfcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `cftp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `obst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `obcp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `rest_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `recp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `cdf_pryp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `cdf_ptyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `cdf_ttyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `cdf_typr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `gcmp_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Company |
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

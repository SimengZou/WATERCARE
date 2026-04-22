# tfgld008

LN tfgld008 Chart of Accounts table, Generated 2026-04-10T19:41:41Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld008` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `leac` |
| **Column count** | 114 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `leac` | `string` | `varchar` | `PK` | `not_null` | (required) Ledger Account. Max length: 12 |
| `subl` | `integer` | `int` |  |  | Account Sublevel |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Ledger Account Description - base language. Max length: 60 |
| `atyp` | `integer` | `int` |  |  | Account Type. Allowed values: 0, 10, 20, 30, 40, 50 |
| `atyp_kw` | `string` | `varchar` |  |  | Account Type (keyword). Allowed values: , tfgld.atyp.balance, tfgld.atyp.profitloss, tfgld.atyp.intercompany, tfgld.atyp.intersegment, tfgld.atyp.text |
| `skey__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `dim1` | `integer` | `int` |  |  | Dimension 1 Used Y/N. Allowed values: 1, 2, 3 |
| `dim1_kw` | `string` | `varchar` |  |  | Dimension 1 Used Y/N (keyword). Allowed values: tfgld.dopt.no, tfgld.dopt.optional, tfgld.dopt.mandatory |
| `dim2` | `integer` | `int` |  |  | Dimension 2 Used Y/N. Allowed values: 1, 2, 3 |
| `dim2_kw` | `string` | `varchar` |  |  | Dimension 2 Used Y/N (keyword). Allowed values: tfgld.dopt.no, tfgld.dopt.optional, tfgld.dopt.mandatory |
| `dim3` | `integer` | `int` |  |  | Dimension 3 Used Y/N. Allowed values: 1, 2, 3 |
| `dim3_kw` | `string` | `varchar` |  |  | Dimension 3 Used Y/N (keyword). Allowed values: tfgld.dopt.no, tfgld.dopt.optional, tfgld.dopt.mandatory |
| `dim4` | `integer` | `int` |  |  | Dimension 4 Used Y/N. Allowed values: 1, 2, 3 |
| `dim4_kw` | `string` | `varchar` |  |  | Dimension 4 Used Y/N (keyword). Allowed values: tfgld.dopt.no, tfgld.dopt.optional, tfgld.dopt.mandatory |
| `dim5` | `integer` | `int` |  |  | Dimension 5 Used Y/N. Allowed values: 1, 2, 3 |
| `dim5_kw` | `string` | `varchar` |  |  | Dimension 5 Used Y/N (keyword). Allowed values: tfgld.dopt.no, tfgld.dopt.optional, tfgld.dopt.mandatory |
| `dim6` | `integer` | `int` |  |  | Dimension 6 Used Y/N. Allowed values: 1, 2, 3 |
| `dim6_kw` | `string` | `varchar` |  |  | Dimension 6 Used Y/N (keyword). Allowed values: tfgld.dopt.no, tfgld.dopt.optional, tfgld.dopt.mandatory |
| `dim7` | `integer` | `int` |  |  | Dimension 7 Used Y/N. Allowed values: 1, 2, 3 |
| `dim7_kw` | `string` | `varchar` |  |  | Dimension 7 Used Y/N (keyword). Allowed values: tfgld.dopt.no, tfgld.dopt.optional, tfgld.dopt.mandatory |
| `dim8` | `integer` | `int` |  |  | Dimension 8 Used Y/N. Allowed values: 1, 2, 3 |
| `dim8_kw` | `string` | `varchar` |  |  | Dimension 8 Used Y/N (keyword). Allowed values: tfgld.dopt.no, tfgld.dopt.optional, tfgld.dopt.mandatory |
| `dim9` | `integer` | `int` |  |  | Dimension 9 Used Y/N. Allowed values: 1, 2, 3 |
| `dim9_kw` | `string` | `varchar` |  |  | Dimension 9 Used Y/N (keyword). Allowed values: tfgld.dopt.no, tfgld.dopt.optional, tfgld.dopt.mandatory |
| `dm10` | `integer` | `int` |  |  | Dimension 10 Used Y/N. Allowed values: 1, 2, 3 |
| `dm10_kw` | `string` | `varchar` |  |  | Dimension 10 Used Y/N (keyword). Allowed values: tfgld.dopt.no, tfgld.dopt.optional, tfgld.dopt.mandatory |
| `dm11` | `integer` | `int` |  |  | Dimension 11 Used Y/N. Allowed values: 1, 2, 3 |
| `dm11_kw` | `string` | `varchar` |  |  | Dimension 11 Used Y/N (keyword). Allowed values: tfgld.dopt.no, tfgld.dopt.optional, tfgld.dopt.mandatory |
| `dm12` | `integer` | `int` |  |  | Dimension 12 Used Y/N. Allowed values: 1, 2, 3 |
| `dm12_kw` | `string` | `varchar` |  |  | Dimension 12 Used Y/N (keyword). Allowed values: tfgld.dopt.no, tfgld.dopt.optional, tfgld.dopt.mandatory |
| `plac` | `string` | `varchar` |  |  | Statutory Parent Account. Max length: 12 |
| `pcac` | `string` | `varchar` |  |  | Complementary Parent Account. Max length: 12 |
| `ifas` | `integer` | `int` |  |  | Fixed Asset Integration. Allowed values: 1, 2, 3, 4 |
| `ifas_kw` | `string` | `varchar` |  |  | Fixed Asset Integration (keyword). Allowed values: tfgld.ifas.investment, tfgld.ifas.maint.costs, tfgld.ifas.disposal, tfgld.ifas.not.applicable |
| `iprj` | `integer` | `int` |  |  | Operations Management Integration. Allowed values: 10, 15, 20, 25, 30, 35, 40, 45, 255 |
| `iprj_kw` | `string` | `varchar` |  |  | Operations Management Integration (keyword). Allowed values: tfgld.intr.pcs, tfgld.intr.proj.costs, tfgld.intr.proj.sales, tfgld.intr.service.pur, tfgld.intr.service.other, tfgld.intr.customer.claim, tfgld.intr.supplier.claim, tfgld.intr.maint.other, tfgld.intr.not.applicable |
| `loco` | `integer` | `int` |  |  | Logistic Company |
| `icom` | `integer` | `int` |  |  | Compression of Transactions. Allowed values: 1, 2 |
| `icom_kw` | `string` | `varchar` |  |  | Compression of Transactions (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pseq` | `integer` | `int` |  |  | Print Sequence |
| `icur` | `integer` | `int` |  |  | Currency Analysis. Allowed values: 1, 2, 3 |
| `icur_kw` | `string` | `varchar` |  |  | Currency Analysis (keyword). Allowed values: tcicur.not.required, tcicur.required, tcicur.difference |
| `uni1` | `string` | `varchar` |  |  | Unit 1. Max length: 3 |
| `uni2` | `string` | `varchar` |  |  | Unit 2. Max length: 3 |
| `bloc` | `integer` | `int` |  |  | Blocking Status. Allowed values: 0, 1, 2, 3 |
| `bloc_kw` | `string` | `varchar` |  |  | Blocking Status (keyword). Allowed values: , tfgld.bloc.free, tfgld.bloc.manual.input, tfgld.bloc.all.input |
| `bfdt` | `date` | `date` |  |  | Blocking Effective From |
| `budt` | `date` | `date` |  |  | Blocking Effective To |
| `cvat` | `string` | `varchar` |  |  | Default Tax Code. Max length: 9 |
| `perc` | `float` | `float` |  |  | Variable Percentage |
| `dbcr` | `integer` | `int` |  |  | Debit/Credit Indicator. Allowed values: 1, 2 |
| `dbcr_kw` | `string` | `varchar` |  |  | Debit/Credit Indicator (keyword). Allowed values: tfgld.dbcr.debit, tfgld.dbcr.credit |
| `lela` | `string` | `varchar` |  |  | Legal Ledger Account. Max length: 12 |
| `ledc__en_us` | `string` | `varchar` |  | `not_null` | (required) Legal Ledger Account Description - base language. Max length: 60 |
| `sear` | `string` | `varchar` |  |  | Source of Earnings. Max length: 9 |
| `alat` | `integer` | `int` |  |  | Allocation Type. Allowed values: 0, 1, 2, 3, 10 |
| `alat_kw` | `string` | `varchar` |  |  | Allocation Type (keyword). Allowed values: , tfgld.atat.primary, tfgld.atat.sec.credit, tfgld.atat.sec.debit, tfgld.atat.not.applicable |
| `duac` | `integer` | `int` |  |  | Dual Accounting Indicator. Allowed values: 1, 2 |
| `duac_kw` | `string` | `varchar` |  |  | Dual Accounting Indicator (keyword). Allowed values: tfgld.duac.statutory, tfgld.duac.complem |
| `ctlm` | `integer` | `int` |  |  | Currency Translation Method. Allowed values: 1, 2, 3 |
| `ctlm_kw` | `string` | `varchar` |  |  | Currency Translation Method (keyword). Allowed values: tfgld.ctlm.local, tfgld.ctlm.reporting, tfgld.ctlm.both |
| `dga1` | `string` | `varchar` |  |  | Destination Gain Account 1. Max length: 12 |
| `dga2` | `string` | `varchar` |  |  | Destination Gain Account 2. Max length: 12 |
| `dla1` | `string` | `varchar` |  |  | Destination Loss Account 1. Max length: 12 |
| `dla2` | `string` | `varchar` |  |  | Destination Loss Account 2. Max length: 12 |
| `ctar` | `integer` | `int` |  |  | Calculate Translation Adjustment. Allowed values: 1, 2 |
| `ctar_kw` | `string` | `varchar` |  |  | Calculate Translation Adjustment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mach` | `integer` | `int` |  |  | Matching. Allowed values: 0, 1, 2 |
| `mach_kw` | `string` | `varchar` |  |  | Matching (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `dmsq` | `integer` | `int` |  |  | Default Matching Sequence |
| `inta` | `integer` | `int` |  |  | Integration Account. Allowed values: 1, 2 |
| `inta_kw` | `string` | `varchar` |  |  | Integration Account (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdca` | `string` | `varchar` |  |  | Currency Differences Contra Account. Max length: 12 |
| `etyp` | `integer` | `int` |  |  | Expense Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22 |
| `etyp_kw` | `string` | `varchar` |  |  | Expense Type (keyword). Allowed values: tcetyp.not.applicable, tcetyp.fees, tcetyp.commissions, tcetyp.brokerage, tcetyp.drawbacks, tcetyp.directors.fees, tcetyp.perform.rights, tcetyp.inventor.rights, tcetyp.wages.salaries, tcetyp.food, tcetyp.housing, tcetyp.coach, tcetyp.other.benefits, tcetyp.flat.rate.benef, tcetyp.reimburse.just, tcetyp.meeting.costs, tcetyp.reduced.rate, tcetyp.waiver.deduct, tcetyp.car, tcetyp.tools.nict, tcetyp.other.advantage, tcetyp.fixed.alloc |
| `dblm` | `integer` | `int` |  |  | Daily Balance. Allowed values: 1, 2 |
| `dblm_kw` | `string` | `varchar` |  |  | Daily Balance (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `blbp` | `integer` | `int` |  |  | Business Partner Balance. Allowed values: 1, 2 |
| `blbp_kw` | `string` | `varchar` |  |  | Business Partner Balance (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cfrs` | `string` | `varchar` |  |  | Cash Flow Reason. Max length: 6 |
| `injb` | `integer` | `int` |  |  | Include in Journal Book. Allowed values: 1, 2 |
| `injb_kw` | `string` | `varchar` |  |  | Include in Journal Book (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `acmp` | `string` | `varchar` |  |  | Account Matching Properties. Max length: 8 |
| `tagp` | `string` | `varchar` |  |  | Target Account Group. Max length: 9 |
| `cfic` | `string` | `varchar` |  |  | Cash Flow Information Code. Max length: 6 |
| `uier` | `integer` | `int` |  |  | Use in ESG Reporting. Allowed values: 1, 2 |
| `uier_kw` | `string` | `varchar` |  |  | Use in ESG Reporting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `text` | `integer` | `int` |  |  | Text |
| `plac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `pcac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `uni1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `uni2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `sear_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld038 Source of Earnings |
| `dga1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `dga2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `dla1_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `dla2_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `cdca_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `cfrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `acmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld157 Account Matching Properties |
| `tagp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld095 Currency Difference Group |
| `cfic_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld260 Cash Flow Information Codes |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

# tccom000

LN tccom000 Implemented Software Components (Companies) table, Generated 2026-04-10T19:41:00Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tccom000` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `indt`, `ncmp` |
| **Column count** | 317 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `indt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Introduction Date |
| `ncmp` | `integer` | `int` | `PK` | `not_null` | (required) Company |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `nama__en_us` | `string` | `varchar` |  | `not_null` | (required) Name - base language. Max length: 35 |
| `ccty` | `string` | `varchar` |  |  | Country. Max length: 3 |
| `cmai` | `string` | `varchar` |  |  | Mailing Number. Max length: 7 |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 3 |
| `cadr` | `string` | `varchar` |  |  | Address Code. Max length: 9 |
| `fovn` | `string` | `varchar` |  |  | Tax Number of Own Company. Max length: 35 |
| `arcc` | `integer` | `int` |  |  | Archive Company. Allowed values: 1, 2 |
| `arcc_kw` | `string` | `varchar` |  |  | Archive Company (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `darc` | `integer` | `int` |  |  | Archive-to Company |
| `adcc` | `integer` | `int` |  |  | Data Lake Company. Allowed values: 1, 2 |
| `adcc_kw` | `string` | `varchar` |  |  | Data Lake Company (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ardc` | `integer` | `int` |  |  | Archive-to Data Lake Company |
| `fini` | `integer` | `int` |  |  | Financials (TF). Allowed values: 1, 2 |
| `fini_kw` | `string` | `varchar` |  |  | Financials (TF) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `frmi` | `integer` | `int` |  |  | Freight Management (FM). Allowed values: 1, 2 |
| `frmi_kw` | `string` | `varchar` |  |  | Freight Management (FM) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prji` | `integer` | `int` |  |  | Project (TP). Allowed values: 1, 2 |
| `prji_kw` | `string` | `varchar` |  |  | Project (TP) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wrhi` | `integer` | `int` |  |  | Warehouse Management (WH). Allowed values: 1, 2 |
| `wrhi_kw` | `string` | `varchar` |  |  | Warehouse Management (WH) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wmbi` | `integer` | `int` |  |  | Factory Track (BR). Allowed values: 1, 2 |
| `wmbi_kw` | `string` | `varchar` |  |  | Factory Track (BR) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cfri` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `cfri_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cpli` | `integer` | `int` |  |  | Enterprise Planning (CP). Allowed values: 1, 2 |
| `cpli_kw` | `string` | `varchar` |  |  | Enterprise Planning (CP) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `srvi` | `integer` | `int` |  |  | Service (TS). Allowed values: 1, 2 |
| `srvi_kw` | `string` | `varchar` |  |  | Service (TS) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cinv` | `integer` | `int` |  |  | Central Invoicing (CI). Allowed values: 1, 2 |
| `cinv_kw` | `string` | `varchar` |  |  | Central Invoicing (CI) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dsri` | `integer` | `int` |  |  | Order Management (TD). Allowed values: 1, 2 |
| `dsri_kw` | `string` | `varchar` |  |  | Order Management (TD) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `spri` | `integer` | `int` |  |  | Services Procurement. Allowed values: 10, 15, 20, 30 |
| `spri_kw` | `string` | `varchar` |  |  | Services Procurement (keyword). Allowed values: tcactf.inactive, tcactf.em.inprep, tcactf.inprep, tcactf.active |
| `qumi` | `integer` | `int` |  |  | Quality Management (QM). Allowed values: 1, 2 |
| `qumi_kw` | `string` | `varchar` |  |  | Quality Management (QM) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ecmi` | `integer` | `int` |  |  | Electronic Commerce (EC). Allowed values: 1, 2 |
| `ecmi_kw` | `string` | `varchar` |  |  | Electronic Commerce (EC) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mnfc` | `integer` | `int` |  |  | Manufacturing (TI). Allowed values: 1, 2 |
| `mnfc_kw` | `string` | `varchar` |  |  | Manufacturing (TI) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tlgi` | `integer` | `int` |  |  | Tool Requirements Planning (TRP). Allowed values: 1, 2 |
| `tlgi_kw` | `string` | `varchar` |  |  | Tool Requirements Planning (TRP) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pcsi` | `integer` | `int` |  |  | Project Control (PCS). Allowed values: 1, 2 |
| `pcsi_kw` | `string` | `varchar` |  |  | Project Control (PCS) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `erac` | `integer` | `int` |  |  | Engineering Revisions. Allowed values: 1, 2 |
| `erac_kw` | `string` | `varchar` |  |  | Engineering Revisions (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rpti` | `integer` | `int` |  |  | Repetitive Manufacturing (RPT). Allowed values: 1, 2 |
| `rpti_kw` | `string` | `varchar` |  |  | Repetitive Manufacturing (RPT) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pcfi` | `integer` | `int` |  |  | Product Configurator (PCF). Allowed values: 1, 2 |
| `pcfi_kw` | `string` | `varchar` |  |  | Product Configurator (PCF) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `unef` | `integer` | `int` |  |  | Unit Effectivity. Allowed values: 1, 2 |
| `unef_kw` | `string` | `varchar` |  |  | Unit Effectivity (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bape` | `integer` | `int` |  |  | People (BP). Allowed values: 1, 2 |
| `bape_kw` | `string` | `varchar` |  |  | People (BP) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `odmu` | `integer` | `int` |  |  | Object Data Management (DM). Allowed values: 1, 2 |
| `odmu_kw` | `string` | `varchar` |  |  | Object Data Management (DM) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `plma` | `integer` | `int` |  |  | Product Lifecycle Management (PD). Allowed values: 1, 2 |
| `plma_kw` | `string` | `varchar` |  |  | Product Lifecycle Management (PD) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `plai` | `integer` | `int` |  |  | Infor SCM Planner. Allowed values: 1, 2 |
| `plai_kw` | `string` | `varchar` |  |  | Infor SCM Planner (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `schi` | `integer` | `int` |  |  | Infor SCM Scheduler. Allowed values: 1, 2 |
| `schi_kw` | `string` | `varchar` |  |  | Infor SCM Scheduler (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `opsi` | `integer` | `int` |  |  | Order Promising Server. Allowed values: 1, 2 |
| `opsi_kw` | `string` | `varchar` |  |  | Order Promising Server (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `asci` | `integer` | `int` |  |  | Assembly. Allowed values: 1, 2 |
| `asci_kw` | `string` | `varchar` |  |  | Assembly (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `asmi` | `integer` | `int` |  |  | Assembly 2.0. Allowed values: 1, 2 |
| `asmi_kw` | `string` | `varchar` |  |  | Assembly 2.0 (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `eust` | `integer` | `int` |  |  | EU Statistical Reporting. Allowed values: 1, 2 |
| `eust_kw` | `string` | `varchar` |  |  | EU Statistical Reporting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `eusl` | `integer` | `int` |  |  | EU Sales Listing. Allowed values: 1, 2 |
| `eusl_kw` | `string` | `varchar` |  |  | EU Sales Listing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `smii` | `integer` | `int` |  |  | Relation Management (SMI). Allowed values: 1, 2 |
| `smii_kw` | `string` | `varchar` |  |  | Relation Management (SMI) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rdes` | `integer` | `int` |  |  | Reference Designators. Allowed values: 1, 2 |
| `rdes_kw` | `string` | `varchar` |  |  | Reference Designators (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dasr` | `integer` | `int` |  |  | DAS 2 Reporting. Allowed values: 1, 2 |
| `dasr_kw` | `string` | `varchar` |  |  | DAS 2 Reporting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `capp` | `integer` | `int` |  |  | Customer Approval. Allowed values: 1, 2 |
| `capp_kw` | `string` | `varchar` |  |  | Customer Approval (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `altm` | `integer` | `int` |  |  | Alternative Materials. Allowed values: 1, 2 |
| `altm_kw` | `string` | `varchar` |  |  | Alternative Materials (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `usup` | `integer` | `int` |  |  | Use Up Material. Allowed values: 1, 2 |
| `usup_kw` | `string` | `varchar` |  |  | Use Up Material (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `deln` | `integer` | `int` |  |  | Delivery Notes. Allowed values: 1, 2 |
| `deln_kw` | `string` | `varchar` |  |  | Delivery Notes (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `stat` | `integer` | `int` |  |  | Purchase/Sales Statistics (STA). Allowed values: 1, 2 |
| `stat_kw` | `string` | `varchar` |  |  | Purchase/Sales Statistics (STA) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uwht` | `integer` | `int` |  |  | Withholding Income Tax and Social Contribution. Allowed values: 1, 2 |
| `uwht_kw` | `string` | `varchar` |  |  | Withholding Income Tax and Social Contribution (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lgid__en_us` | `string` | `varchar` |  | `not_null` | (required) Chamber of Commerce ID - base language. Max length: 20 |
| `duns` | `integer` | `int` |  |  | DUNS Number |
| `fidn` | `string` | `varchar` |  |  | Fiscal Identification. Max length: 20 |
| `gtro` | `integer` | `int` |  |  | Grand Total Rounding. Allowed values: 1, 2 |
| `gtro_kw` | `string` | `varchar` |  |  | Grand Total Rounding (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lnwi` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `lnwi_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `taxd` | `integer` | `int` |  |  | Tax. Allowed values: 1, 2 |
| `taxd_kw` | `string` | `varchar` |  |  | Tax (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mpni` | `integer` | `int` |  |  | Manufacturer Part Numbers. Allowed values: 1, 2 |
| `mpni_kw` | `string` | `varchar` |  |  | Manufacturer Part Numbers (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `owni` | `integer` | `int` |  |  | Ownership Internal. Allowed values: 1, 2 |
| `owni_kw` | `string` | `varchar` |  |  | Ownership Internal (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `owne` | `integer` | `int` |  |  | Ownership External. Allowed values: 1, 2 |
| `owne_kw` | `string` | `varchar` |  |  | Ownership External (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `smfs` | `integer` | `int` |  |  | Customer Furnished Materials. Allowed values: 1, 2 |
| `smfs_kw` | `string` | `varchar` |  |  | Customer Furnished Materials (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `smfm` | `integer` | `int` |  |  | Subcontracting with Material Flow. Allowed values: 1, 2 |
| `smfm_kw` | `string` | `varchar` |  |  | Subcontracting with Material Flow (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ssmf` | `integer` | `int` |  |  | Subcontracting with Material Flow - Service. Allowed values: 1, 2 |
| `ssmf_kw` | `string` | `varchar` |  |  | Subcontracting with Material Flow - Service (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `alhp` | `integer` | `int` |  |  | Demand Pegging. Allowed values: 1, 2 |
| `alhp_kw` | `string` | `varchar` |  |  | Demand Pegging (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vmis` | `integer` | `int` |  |  | VMI (supplier side). Allowed values: 1, 2 |
| `vmis_kw` | `string` | `varchar` |  |  | VMI (supplier side) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vmic` | `integer` | `int` |  |  | VMI (customer side). Allowed values: 1, 2 |
| `vmic_kw` | `string` | `varchar` |  |  | VMI (customer side) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ucsl` | `integer` | `int` |  |  | Use Confirmation (Sales). Allowed values: 1, 2 |
| `ucsl_kw` | `string` | `varchar` |  |  | Use Confirmation (Sales) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ucpu` | `integer` | `int` |  |  | Use Confirmation (Purchase). Allowed values: 1, 2 |
| `ucpu_kw` | `string` | `varchar` |  |  | Use Confirmation (Purchase) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `trcn` | `integer` | `int` |  |  | Terms and Conditions (TRM). Allowed values: 1, 2 |
| `trcn_kw` | `string` | `varchar` |  |  | Terms and Conditions (TRM) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `csli` | `integer` | `int` |  |  | Customer and Supplier List Italy. Allowed values: 1, 2 |
| `csli_kw` | `string` | `varchar` |  |  | Customer and Supplier List Italy (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `eupl` | `integer` | `int` |  |  | EU Purchase Listing. Allowed values: 1, 2 |
| `eupl_kw` | `string` | `varchar` |  |  | EU Purchase Listing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ffpl` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `ffpl_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cbtp` | `string` | `varchar` |  |  | Business Partner Type. Max length: 3 |
| `cbrn` | `string` | `varchar` |  |  | Line of Business. Max length: 6 |
| `repr` | `string` | `varchar` |  |  | Representative. Max length: 9 |
| `crid__en_us` | `string` | `varchar` |  | `not_null` | (required) Creditor Identifier - base language. Max length: 40 |
| `beid__en_us` | `string` | `varchar` |  | `not_null` | (required) Business Entity Identifier - base language. Max length: 15 |
| `bgci` | `integer` | `int` |  |  | Budget Control. Allowed values: 1, 2 |
| `bgci_kw` | `string` | `varchar` |  |  | Budget Control (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `acfg` | `integer` | `int` |  |  | Infor Configurator (CPQ). Allowed values: 1, 2 |
| `acfg_kw` | `string` | `varchar` |  |  | Infor Configurator (CPQ) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lcsi` | `integer` | `int` |  |  | Landed Costs. Allowed values: 1, 2 |
| `lcsi_kw` | `string` | `varchar` |  |  | Landed Costs (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lrus` | `integer` | `int` |  |  | Functionality for Russia. Allowed values: 1, 2 |
| `lrus_kw` | `string` | `varchar` |  |  | Functionality for Russia (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lpol` | `integer` | `int` |  |  | Functionality for Poland. Allowed values: 1, 2 |
| `lpol_kw` | `string` | `varchar` |  |  | Functionality for Poland (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lcze` | `integer` | `int` |  |  | Functionality for Czech Republic. Allowed values: 1, 2 |
| `lcze_kw` | `string` | `varchar` |  |  | Functionality for Czech Republic (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lnor` | `integer` | `int` |  |  | Functionality for Norway. Allowed values: 1, 2 |
| `lnor_kw` | `string` | `varchar` |  |  | Functionality for Norway (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lsvk` | `integer` | `int` |  |  | Functionality for Slovakia. Allowed values: 1, 2 |
| `lsvk_kw` | `string` | `varchar` |  |  | Functionality for Slovakia (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lmys` | `integer` | `int` |  |  | Functionality for Malaysia. Allowed values: 1, 2 |
| `lmys_kw` | `string` | `varchar` |  |  | Functionality for Malaysia (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bulg` | `integer` | `int` |  |  | Functionality for Bulgaria. Allowed values: 1, 2 |
| `bulg_kw` | `string` | `varchar` |  |  | Functionality for Bulgaria (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lbra` | `integer` | `int` |  |  | Functionality for Brazil. Allowed values: 1, 2 |
| `lbra_kw` | `string` | `varchar` |  |  | Functionality for Brazil (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lesp` | `integer` | `int` |  |  | Functionality for Spain. Allowed values: 1, 2 |
| `lesp_kw` | `string` | `varchar` |  |  | Functionality for Spain (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lmex` | `integer` | `int` |  |  | Functionality for Mexico. Allowed values: 1, 2 |
| `lmex_kw` | `string` | `varchar` |  |  | Functionality for Mexico (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ldeu` | `integer` | `int` |  |  | Functionality for Germany. Allowed values: 1, 2 |
| `ldeu_kw` | `string` | `varchar` |  |  | Functionality for Germany (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lisr` | `integer` | `int` |  |  | Functionality For Israel. Allowed values: 1, 2 |
| `lisr_kw` | `string` | `varchar` |  |  | Functionality For Israel (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lrom` | `integer` | `int` |  |  | Functionality for Romania. Allowed values: 1, 2 |
| `lrom_kw` | `string` | `varchar` |  |  | Functionality for Romania (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lchn` | `integer` | `int` |  |  | Functionality for China. Allowed values: 1, 2 |
| `lchn_kw` | `string` | `varchar` |  |  | Functionality for China (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lchl` | `integer` | `int` |  |  | Functionality for Chile. Allowed values: 1, 2 |
| `lchl_kw` | `string` | `varchar` |  |  | Functionality for Chile (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ltur` | `integer` | `int` |  |  | Functionality for Turkey. Allowed values: 1, 2 |
| `ltur_kw` | `string` | `varchar` |  |  | Functionality for Turkey (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `larg` | `integer` | `int` |  |  | Functionality for Argentina. Allowed values: 1, 2 |
| `larg_kw` | `string` | `varchar` |  |  | Functionality for Argentina (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lhun` | `integer` | `int` |  |  | Functionality for Hungary. Allowed values: 1, 2 |
| `lhun_kw` | `string` | `varchar` |  |  | Functionality for Hungary (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lpor` | `integer` | `int` |  |  | Functionality for Portugal. Allowed values: 1, 2 |
| `lpor_kw` | `string` | `varchar` |  |  | Functionality for Portugal (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lche` | `integer` | `int` |  |  | Functionality for Switzerland. Allowed values: 1, 2 |
| `lche_kw` | `string` | `varchar` |  |  | Functionality for Switzerland (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lper` | `integer` | `int` |  |  | Functionality for Peru. Allowed values: 1, 2 |
| `lper_kw` | `string` | `varchar` |  |  | Functionality for Peru (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ltha` | `integer` | `int` |  |  | Functionality for Thailand. Allowed values: 1, 2 |
| `ltha_kw` | `string` | `varchar` |  |  | Functionality for Thailand (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lago` | `integer` | `int` |  |  | Functionality for Angola. Allowed values: 1, 2 |
| `lago_kw` | `string` | `varchar` |  |  | Functionality for Angola (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lind` | `integer` | `int` |  |  | Functionality for India. Allowed values: 1, 2 |
| `lind_kw` | `string` | `varchar` |  |  | Functionality for India (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lidn` | `integer` | `int` |  |  | Functionality for Indonesia. Allowed values: 1, 2 |
| `lidn_kw` | `string` | `varchar` |  |  | Functionality for Indonesia (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lvnm` | `integer` | `int` |  |  | Functionality for Vietnam. Allowed values: 1, 2 |
| `lvnm_kw` | `string` | `varchar` |  |  | Functionality for Vietnam (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lcol` | `integer` | `int` |  |  | Functionality for Colombia. Allowed values: 1, 2 |
| `lcol_kw` | `string` | `varchar` |  |  | Functionality for Colombia (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lita` | `integer` | `int` |  |  | Functionality for Italy. Allowed values: 1, 2 |
| `lita_kw` | `string` | `varchar` |  |  | Functionality for Italy (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lsau` | `integer` | `int` |  |  | Functionality for Saudi Arabia. Allowed values: 1, 2 |
| `lsau_kw` | `string` | `varchar` |  |  | Functionality for Saudi Arabia (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lsvn` | `integer` | `int` |  |  | Functionality for Slovenia. Allowed values: 1, 2 |
| `lsvn_kw` | `string` | `varchar` |  |  | Functionality for Slovenia (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lhrv` | `integer` | `int` |  |  | Functionality for Croatia. Allowed values: 1, 2 |
| `lhrv_kw` | `string` | `varchar` |  |  | Functionality for Croatia (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lsrb` | `integer` | `int` |  |  | Functionality for Serbia. Allowed values: 1, 2 |
| `lsrb_kw` | `string` | `varchar` |  |  | Functionality for Serbia (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lphl` | `integer` | `int` |  |  | Functionality for Philippines. Allowed values: 1, 2 |
| `lphl_kw` | `string` | `varchar` |  |  | Functionality for Philippines (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lgbr` | `integer` | `int` |  |  | Functionality for United Kingdom. Allowed values: 1, 2 |
| `lgbr_kw` | `string` | `varchar` |  |  | Functionality for United Kingdom (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lfra` | `integer` | `int` |  |  | Functionality for France. Allowed values: 1, 2 |
| `lfra_kw` | `string` | `varchar` |  |  | Functionality for France (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lkor` | `integer` | `int` |  |  | Functionality for South Korea. Allowed values: 1, 2 |
| `lkor_kw` | `string` | `varchar` |  |  | Functionality for South Korea (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ppeg` | `integer` | `int` |  |  | Project Pegging. Allowed values: 1, 2 |
| `ppeg_kw` | `string` | `varchar` |  |  | Project Pegging (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `slbp` | `integer` | `int` |  |  | Self-Billing (Purchase). Allowed values: 1, 2 |
| `slbp_kw` | `string` | `varchar` |  |  | Self-Billing (Purchase) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `slbs` | `integer` | `int` |  |  | Self-Billing (Sales). Allowed values: 1, 2 |
| `slbs_kw` | `string` | `varchar` |  |  | Self-Billing (Sales) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ilwh` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `ilwh_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `siac__en_us` | `string` | `varchar` |  | `not_null` | (required) SIA Company Code - base language. Max length: 5 |
| `cbrk` | `integer` | `int` |  |  | Costing Breaks. Allowed values: 1, 2 |
| `cbrk_kw` | `string` | `varchar` |  |  | Costing Breaks (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `spsp` | `integer` | `int` |  |  | Supplier Stage Payments. Allowed values: 1, 2 |
| `spsp_kw` | `string` | `varchar` |  |  | Supplier Stage Payments (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mprs` | `integer` | `int` |  |  | Material Pricing. Allowed values: 1, 2 |
| `mprs_kw` | `string` | `varchar` |  |  | Material Pricing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `hcst` | `integer` | `int` |  |  | Show Hours in Costing. Allowed values: 1, 2 |
| `hcst_kw` | `string` | `varchar` |  |  | Show Hours in Costing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mfsi` | `integer` | `int` |  |  | Mobile Field Service Implemented. Allowed values: 1, 2 |
| `mfsi_kw` | `string` | `varchar` |  |  | Mobile Field Service Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `itri` | `integer` | `int` |  |  | Intercompany Trade. Allowed values: 1, 2 |
| `itri_kw` | `string` | `varchar` |  |  | Intercompany Trade (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ptri` | `integer` | `int` |  |  | Point in Time Revenue Recognition. Allowed values: 1, 2 |
| `ptri_kw` | `string` | `varchar` |  |  | Point in Time Revenue Recognition (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `asec` | `integer` | `int` |  |  | Authorization and Security. Allowed values: 0, 1, 2 |
| `asec_kw` | `string` | `varchar` |  |  | Authorization and Security (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `grpl` | `integer` | `int` |  |  | Resource Planning. Allowed values: 1, 2 |
| `grpl_kw` | `string` | `varchar` |  |  | Resource Planning (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `gtci` | `integer` | `int` |  |  | Trade Management. Allowed values: 1, 2 |
| `gtci_kw` | `string` | `varchar` |  |  | Trade Management (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `itpd` | `integer` | `int` |  |  | Item Type Product. Allowed values: 10, 15, 20, 30 |
| `itpd_kw` | `string` | `varchar` |  |  | Item Type Product (keyword). Allowed values: tcactf.inactive, tcactf.em.inprep, tcactf.inprep, tcactf.active |
| `jspm` | `integer` | `int` |  |  | Job Shop by Site. Allowed values: 10, 15, 20, 30 |
| `jspm_kw` | `string` | `varchar` |  |  | Job Shop by Site (keyword). Allowed values: tcactf.inactive, tcactf.em.inprep, tcactf.inprep, tcactf.active |
| `mstc` | `integer` | `int` |  |  | Standard Cost by Enterprise Unit. Allowed values: 10, 15, 20, 30 |
| `mstc_kw` | `string` | `varchar` |  |  | Standard Cost by Enterprise Unit (keyword). Allowed values: tcactf.inactive, tcactf.em.inprep, tcactf.inprep, tcactf.active |
| `pcmd` | `integer` | `int` |  |  | Planning Cluster Mandatory. Allowed values: 10, 15, 20, 30 |
| `pcmd_kw` | `string` | `varchar` |  |  | Planning Cluster Mandatory (keyword). Allowed values: tcactf.inactive, tcactf.em.inprep, tcactf.inprep, tcactf.active |
| `simd` | `integer` | `int` |  |  | Sites. Allowed values: 10, 15, 20, 30 |
| `simd_kw` | `string` | `varchar` |  |  | Sites (keyword). Allowed values: tcactf.inactive, tcactf.em.inprep, tcactf.inprep, tcactf.active |
| `prci` | `integer` | `int` |  |  | Product Classification. Allowed values: 1, 2 |
| `prci_kw` | `string` | `varchar` |  |  | Product Classification (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tati` | `integer` | `int` |  |  | Turnaround Time. Allowed values: 1, 2 |
| `tati_kw` | `string` | `varchar` |  |  | Turnaround Time (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bcmi` | `integer` | `int` |  |  | Business Communication. Allowed values: 1, 2 |
| `bcmi_kw` | `string` | `varchar` |  |  | Business Communication (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mejs` | `integer` | `int` |  |  | MES Reporting for Job Shop. Allowed values: 1, 2 |
| `mejs_kw` | `string` | `varchar` |  |  | MES Reporting for Job Shop (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `psch` | `integer` | `int` |  |  | Production Scheduler. Allowed values: 1, 2 |
| `psch_kw` | `string` | `varchar` |  |  | Production Scheduler (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `stbi` | `integer` | `int` |  |  | Scan To Book. Allowed values: 1, 2 |
| `stbi_kw` | `string` | `varchar` |  |  | Scan To Book (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `eaas` | `integer` | `int` |  |  | Equipment as a Service. Allowed values: 1, 2 |
| `eaas_kw` | `string` | `varchar` |  |  | Equipment as a Service (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rsbs` | `integer` | `int` |  |  | Resources by Site. Allowed values: 10, 15, 20, 30 |
| `rsbs_kw` | `string` | `varchar` |  |  | Resources by Site (keyword). Allowed values: tcactf.inactive, tcactf.em.inprep, tcactf.inprep, tcactf.active |
| `scbs` | `integer` | `int` |  |  | Sourcing by Site. Allowed values: 10, 15, 20, 30 |
| `scbs_kw` | `string` | `varchar` |  |  | Sourcing by Site (keyword). Allowed values: tcactf.inactive, tcactf.em.inprep, tcactf.inprep, tcactf.active |
| `inf1__en_us` | `string` | `varchar` |  | `not_null` | (required) Company Information 1 - base language. Max length: 100 |
| `inf2__en_us` | `string` | `varchar` |  | `not_null` | (required) Company Information 2 - base language. Max length: 100 |
| `inf3__en_us` | `string` | `varchar` |  | `not_null` | (required) Company Information 3 - base language. Max length: 100 |
| `inf4__en_us` | `string` | `varchar` |  | `not_null` | (required) Company Information 4 - base language. Max length: 100 |
| `xtmm` | `integer` | `int` |  |  | Attendance. Allowed values: 0, 1, 2 |
| `xtmm_kw` | `string` | `varchar` |  |  | Attendance (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `clbi` | `integer` | `int` |  |  | Collaboration Portal. Allowed values: 1, 2 |
| `clbi_kw` | `string` | `varchar` |  |  | Collaboration Portal (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cfbs` | `integer` | `int` |  |  | Configurations by Site. Allowed values: 10, 15, 20, 30 |
| `cfbs_kw` | `string` | `varchar` |  |  | Configurations by Site (keyword). Allowed values: tcactf.inactive, tcactf.em.inprep, tcactf.inprep, tcactf.active |
| `eqbu` | `integer` | `int` |  |  | Buying Equipment. Allowed values: 10, 15, 20, 30 |
| `eqbu_kw` | `string` | `varchar` |  |  | Buying Equipment (keyword). Allowed values: tcactf.inactive, tcactf.em.inprep, tcactf.inprep, tcactf.active |
| `corg` | `integer` | `int` |  |  | Country of Origin. Allowed values: 1, 2 |
| `corg_kw` | `string` | `varchar` |  |  | Country of Origin (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `inpr` | `integer` | `int` |  |  | Inward Processing. Allowed values: 1, 2 |
| `inpr_kw` | `string` | `varchar` |  |  | Inward Processing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pdim` | `integer` | `int` |  |  | Product Dimensions (DS). Allowed values: 1, 2 |
| `pdim_kw` | `string` | `varchar` |  |  | Product Dimensions (DS) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pedi` | `integer` | `int` |  |  | Import Reference. Allowed values: 1, 2 |
| `pedi_kw` | `string` | `varchar` |  |  | Import Reference (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `intr` | `integer` | `int` |  |  | Inventory Tracking. Allowed values: 1, 2 |
| `intr_kw` | `string` | `varchar` |  |  | Inventory Tracking (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fstk` | `integer` | `int` |  |  | Floor Stock. Allowed values: 1, 2 |
| `fstk_kw` | `string` | `varchar` |  |  | Floor Stock (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mesa` | `integer` | `int` |  |  | MES Reporting for Assembly. Allowed values: 1, 2 |
| `mesa_kw` | `string` | `varchar` |  |  | MES Reporting for Assembly (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `plnp` | `integer` | `int` |  |  | Infor Planner+. Allowed values: 1, 2 |
| `plnp_kw` | `string` | `varchar` |  |  | Infor Planner+ (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `schp` | `integer` | `int` |  |  | Infor Scheduler+. Allowed values: 0, 1, 2 |
| `schp_kw` | `string` | `varchar` |  |  | Infor Scheduler+ (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `esag` | `integer` | `int` |  |  | Environmental, Social and Governance (ESG). Allowed values: 1, 2 |
| `esag_kw` | `string` | `varchar` |  |  | Environmental, Social and Governance (ESG) (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccty_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
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

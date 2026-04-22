# tsctm110

LN tsctm110 Configuration Lines table, Generated 2026-04-10T19:42:30Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsctm110` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `term`, `cfln` |
| **Column count** | 112 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `term` | `integer` | `int` | `PK` | `not_null` | (required) Contract Terms |
| `cfln` | `integer` | `int` | `PK` | `not_null` | (required) Configuration Line |
| `cgru` | `string` | `varchar` |  |  | Run Identification. Max length: 3 |
| `clst` | `string` | `varchar` |  |  | Installation Group. Max length: 9 |
| `item` | `string` | `varchar` |  |  | Item. Max length: 47 |
| `sern` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `nitm` | `integer` | `int` |  |  | Number of Items |
| `cltp` | `integer` | `int` |  |  | Type of Configuration Line. Allowed values: 1, 2, 3 |
| `cltp_kw` | `string` | `varchar` |  |  | Type of Configuration Line (keyword). Allowed values: tsctm.cltp.normal, tsctm.cltp.total, tsctm.cltp.calculation |
| `crac` | `string` | `varchar` |  |  | Reference Activity. Max length: 16 |
| `nrac` | `integer` | `int` |  |  | Number of Activities |
| `cctp` | `string` | `varchar` |  |  | Coverage Type. Max length: 3 |
| `tmdu` | `float` | `float` |  |  | Duration |
| `ccte` | `string` | `varchar` |  |  | Contract Template. Max length: 16 |
| `prmt` | `integer` | `int` |  |  | Pricing Method. Allowed values: 5, 10, 15, 20 |
| `prmt_kw` | `string` | `varchar` |  |  | Pricing Method (keyword). Allowed values: tsctm.prmt.sales, tsctm.prmt.budget, tsctm.prmt.price, tsctm.prmt.template.price |
| `perc` | `float` | `float` |  |  | Percentage of Sales Value |
| `ctin` | `string` | `varchar` |  |  | Installment Template. Max length: 3 |
| `efdt` | `date` | `date` |  |  | Effective Date |
| `cddt` | `date` | `date` |  |  | Contract Discount Date |
| `exdt` | `date` | `date` |  |  | Expiry Date |
| `csco` | `string` | `varchar` |  |  | Service Contract. Max length: 9 |
| `cchn` | `integer` | `int` |  |  | Contract Change Number |
| `cspr` | `float` | `float` |  |  | Calculated Sales Price |
| `cnrp` | `integer` | `int` |  |  | Number of Periods Calculated Price |
| `cpru` | `integer` | `int` |  |  | Period Unit Calculated Price. Allowed values: 5, 10, 15, 20, 25 |
| `cpru_kw` | `string` | `varchar` |  |  | Period Unit Calculated Price (keyword). Allowed values: tsmdm.peru.day, tsmdm.peru.week, tsmdm.peru.month, tsmdm.peru.quarter, tsmdm.peru.year |
| `rspr` | `float` | `float` |  |  | Sales Price |
| `nrpe` | `integer` | `int` |  |  | Number of Periods |
| `peru` | `integer` | `int` |  |  | Period Unit. Allowed values: 5, 10, 15, 20, 25 |
| `peru_kw` | `string` | `varchar` |  |  | Period Unit (keyword). Allowed values: tsmdm.peru.day, tsmdm.peru.week, tsmdm.peru.month, tsmdm.peru.quarter, tsmdm.peru.year |
| `csmt` | `float` | `float` |  |  | Calculated Sales Amount |
| `rsmt` | `float` | `float` |  |  | Sales Amount |
| `dipc` | `float` | `float` |  |  | Surcharge Percentage |
| `dimt` | `float` | `float` |  |  | Surcharge Amount |
| `gsmt` | `float` | `float` |  |  | Gross Sales Amount |
| `upmt` | `float` | `float` |  |  | Excluded by Effectivity Amount |
| `ccds` | `string` | `varchar` |  |  | Contract Discount Scheme. Max length: 3 |
| `cdmt` | `float` | `float` |  |  | Contract Discount Scheme Amount |
| `nsmt` | `float` | `float` |  |  | Net Sales Amount |
| `ccmt_1` | `float` | `float` |  |  | Calculated Cost Amount |
| `ccmt_2` | `float` | `float` |  |  | Calculated Cost Amount |
| `ccmt_3` | `float` | `float` |  |  | Calculated Cost Amount |
| `camt_1` | `float` | `float` |  |  | Cost Amount |
| `camt_2` | `float` | `float` |  |  | Cost Amount |
| `camt_3` | `float` | `float` |  |  | Cost Amount |
| `erfa` | `float` | `float` |  |  | Earned Revenue Factor |
| `mexp` | `integer` | `int` |  |  | Marked for Expiry. Allowed values: 1, 2 |
| `mexp_kw` | `string` | `varchar` |  |  | Marked for Expiry (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `amdy` | `float` | `float` |  |  | Amount per Day |
| `cody_1` | `float` | `float` |  |  | Cost per Day |
| `cody_2` | `float` | `float` |  |  | Cost per Day |
| `cody_3` | `float` | `float` |  |  | Cost per Day |
| `cprj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `pcac` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `csar` | `string` | `varchar` |  |  | Service Area. Max length: 3 |
| `ptrm` | `integer` | `int` |  |  | Pricing Terms. Allowed values: 1, 2 |
| `ptrm_kw` | `string` | `varchar` |  |  | Pricing Terms (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ctrm` | `integer` | `int` |  |  | Coverage Terms. Allowed values: 1, 2 |
| `ctrm_kw` | `string` | `varchar` |  |  | Coverage Terms (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tmpr` | `integer` | `int` |  |  | Time and Material Sales Prices. Allowed values: 1, 2 |
| `tmpr_kw` | `string` | `varchar` |  |  | Time and Material Sales Prices (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fxpr` | `integer` | `int` |  |  | Fixed Sales Prices. Allowed values: 1, 2 |
| `fxpr_kw` | `string` | `varchar` |  |  | Fixed Sales Prices (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ortp` | `integer` | `int` |  |  | Originating Order Type. Allowed values: 5, 10, 15, 99 |
| `ortp_kw` | `string` | `varchar` |  |  | Originating Order Type (keyword). Allowed values: tscfg.ortp.sales, tscfg.ortp.deliverable, tscfg.ortp.project, tscfg.ortp.not.applicable |
| `srno` | `string` | `varchar` |  |  | Originating Order. Max length: 9 |
| `cnln` | `string` | `varchar` |  |  | Originating Contract Line. Max length: 8 |
| `spno` | `integer` | `int` |  |  | Originating Order Line |
| `sdno` | `integer` | `int` |  |  | Originating Detail Line |
| `docn` | `string` | `varchar` |  |  | Turnaround Time Document. Max length: 9 |
| `cncd` | `string` | `varchar` |  |  | Conformance Reporting. Max length: 6 |
| `pbsm` | `integer` | `int` |  |  | Problem Solving Method. Allowed values: 10, 20, 30, 90, 100 |
| `pbsm_kw` | `string` | `varchar` |  |  | Problem Solving Method (keyword). Allowed values: tcmcs.pbsm.8d, tcmcs.pbsm.a3, tcmcs.pbsm.chlt, tcmcs.pbsm.other, tcmcs.pbsm.na |
| `cusc` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `cptp` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2, 99 |
| `cptp_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tsctm.cptp.time.material, tsctm.cptp.fixed.price, tsctm.cptp.not.applicable |
| `frpr` | `float` | `float` |  |  | Obsolete |
| `txta` | `integer` | `int` |  |  | Text |
| `clst_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsbsc100 Installation Group |
| `item_sern_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tscfg200 Serialized Items |
| `item_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm200 Items - Service |
| `crac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsacm101 Reference Activities / Master Routing (Option)s |
| `cctp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm035 Coverage Types |
| `ccte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm050 Contract Templates |
| `ctin_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm410 Installment Templates |
| `csco_cchn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm320 Contract Changes |
| `csco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm300 Service Contracts |
| `ccds_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsctm010 Contract Discount Schemes |
| `cprj_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs052 Projects |
| `csar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tsmdm105 Service Areas |
| `docn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcttm100 Turnaround Time Documents |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `rsmt_homc` | `float` | `float` |  |  | CC: Sales Amount in Local Currency |
| `rsmt_rpc1` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 1 |
| `rsmt_rpc2` | `float` | `float` |  |  | CC: Sales Amount in Reporting Currency 2 |
| `rsmt_refc` | `float` | `float` |  |  | CC: Sales Amount in Reference Currency |
| `rsmt_dwhc` | `float` | `float` |  |  | CC: Sales Amount in Data Warehouse Currency |
| `camt_cntc` | `float` | `float` |  |  | CC: Cost Amount in Contract Currency |
| `camt_refc` | `float` | `float` |  |  | CC: Cost Amount in Reference Currency |
| `camt_dwhc` | `float` | `float` |  |  | CC: Cost Amount in Data Warehouse Currency |
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

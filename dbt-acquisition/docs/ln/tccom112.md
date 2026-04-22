# tccom112

LN tccom112 Invoice-to Business Partners table, Generated 2026-04-10T19:41:02Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tccom112` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `itbp`, `cofc` |
| **Column count** | 123 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `itbp` | `string` | `varchar` | `PK` | `not_null` | (required) Invoice-to Business Partner. Max length: 9 |
| `cofc` | `string` | `varchar` | `PK` | `not_null` | (required) Department. Max length: 6 |
| `itst` | `integer` | `int` |  |  | Business Partner Status. Allowed values: 1, 2, 3, 254 |
| `itst_kw` | `string` | `varchar` |  |  | Business Partner Status (keyword). Allowed values: tccom.itst.active, tccom.itst.inactive, tccom.itst.doubtful, tccom.itst.not.specified |
| `stdt` | `timestamp` | `timestamp_ntz` |  |  | Start Date |
| `endt` | `timestamp` | `timestamp_ntz` |  |  | End Date |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 3 |
| `cbps` | `string` | `varchar` |  |  | Business Partner Signal. Max length: 3 |
| `cbtp` | `string` | `varchar` |  |  | Business Partner Type. Max length: 3 |
| `cadr` | `string` | `varchar` |  |  | Address Code. Max length: 9 |
| `telp` | `string` | `varchar` |  |  | Phone Number. Max length: 32 |
| `tlcy` | `string` | `varchar` |  |  | Country for Phone Number. Max length: 6 |
| `tlci` | `string` | `varchar` |  |  | City/Area Code for Phone Number. Max length: 15 |
| `tlln` | `string` | `varchar` |  |  | Local Number for Phone Number. Max length: 32 |
| `tlex` | `string` | `varchar` |  |  | Extension for Phone Number. Max length: 15 |
| `tefx` | `string` | `varchar` |  |  | Fax Number. Max length: 32 |
| `tfcy` | `string` | `varchar` |  |  | Country for Fax Number. Max length: 6 |
| `tfci` | `string` | `varchar` |  |  | City/Area Code for Fax Number. Max length: 15 |
| `tfln` | `string` | `varchar` |  |  | Local Number for Fax Number. Max length: 32 |
| `tfex` | `string` | `varchar` |  |  | Extension for Fax Number. Max length: 15 |
| `ccnt` | `string` | `varchar` |  |  | Contact. Max length: 9 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `cfcg` | `string` | `varchar` |  |  | Financial Customer Group. Max length: 3 |
| `mstm` | `string` | `varchar` |  |  | Statement Method. Max length: 3 |
| `chin` | `integer` | `int` |  |  | Charge Interest. Allowed values: 1, 2 |
| `chin_kw` | `string` | `varchar` |  |  | Charge Interest (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ncin` | `integer` | `int` |  |  | Number of Extra Invoice Copies |
| `cinm` | `string` | `varchar` |  |  | Invoicing Method. Max length: 3 |
| `cidm` | `string` | `varchar` |  |  | Invoice Delivery Method. Max length: 3 |
| `mcod` | `string` | `varchar` |  |  | Match Code. Max length: 3 |
| `infq` | `integer` | `int` |  |  | Invoice Interval |
| `lidt` | `timestamp` | `timestamp_ntz` |  |  | Date Invoiced |
| `infr` | `integer` | `int` |  |  | Invoice for Freight Based On. Allowed values: 5, 10, 15, 20 |
| `infr_kw` | `string` | `varchar` |  |  | Invoice for Freight Based On (keyword). Allowed values: tccom.infr.estimate, tccom.infr.actual, tccom.infr.costplus, tccom.infr.not.applic |
| `adpc` | `float` | `float` |  |  | Additional Percentage |
| `adam` | `float` | `float` |  |  | Additional Amount |
| `crat` | `string` | `varchar` |  |  | Credit Rating. Max length: 3 |
| `ccra` | `string` | `varchar` |  |  | Credit Analyst. Max length: 9 |
| `crbu` | `string` | `varchar` |  |  | Credit Bureau Reference Number. Max length: 30 |
| `crlr` | `float` | `float` |  |  | Credit Limit |
| `hcru` | `float` | `float` |  |  | Highest Credit Used Since Last Credit Review |
| `dlcr` | `timestamp` | `timestamp_ntz` |  |  | Date of Last Credit Review |
| `clin` | `float` | `float` |  |  | Credit Limit Insured |
| `ccic` | `string` | `varchar` |  |  | Credit Insurance Company. Max length: 3 |
| `rfci` | `string` | `varchar` |  |  | Reference Credit Insurance Company. Max length: 15 |
| `edci` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date of Insured Credit Limit |
| `tamd` | `float` | `float` |  |  | Tolerance Amount for Due Invoices |
| `tpcd` | `float` | `float` |  |  | Tolerance Percentage for Due Invoices |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `rpay` | `string` | `varchar` |  |  | Payment Terms for Credit Notes. Max length: 3 |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `rcrs` | `string` | `varchar` |  |  | Late Payment Surcharge for Credit Notes. Max length: 3 |
| `pfbp` | `string` | `varchar` |  |  | Pay-by Business Partner. Max length: 9 |
| `paym` | `string` | `varchar` |  |  | Payment Method. Max length: 3 |
| `rpym` | `string` | `varchar` |  |  | Payment Method for Credit Notes. Max length: 3 |
| `dim1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `dim2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `dim3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `dim4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `dim5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `dim6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `dim7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `dim8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `dim9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `dm10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `dm11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `dm12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `sirn` | `string` | `varchar` |  |  | SIREN Code. Max length: 9 |
| `ecod` | `string` | `varchar` |  |  | Establishment Code. Max length: 5 |
| `lkst` | `integer` | `int` |  |  | Send MBI. Allowed values: 1, 2 |
| `lkst_kw` | `string` | `varchar` |  |  | Send MBI (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `clmt` | `string` | `varchar` |  |  | Closing Method. Max length: 3 |
| `bpcl` | `string` | `varchar` |  |  | Business Partner Tax Classification. Max length: 12 |
| `inpl` | `string` | `varchar` |  |  | Installment Plan. Max length: 9 |
| `seak__en_us` | `string` | `varchar` |  | `not_null` | (required) Search Key - base language. Max length: 16 |
| `cvyn` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `cvyn_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `user` | `string` | `varchar` |  |  | Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `lmus` | `string` | `varchar` |  |  | Last Modified by. Max length: 16 |
| `lmdt` | `timestamp` | `timestamp_ntz` |  |  | Last Modification Date |
| `ircd` | `string` | `varchar` |  |  | Interest Rate Code. Max length: 3 |
| `bicy` | `string` | `varchar` |  |  | Billing Cycle. Max length: 9 |
| `qrbl` | `integer` | `int` |  |  | QR-billing. Allowed values: 10, 20, 254, 255 |
| `qrbl_kw` | `string` | `varchar` |  |  | QR-billing (keyword). Allowed values: tccom.qrbl.iban, tccom.qrbl.qr.iban, tccom.qrbl.not.specified, tccom.qrbl.not.appl |
| `txta` | `integer` | `int` |  |  | Text |
| `itbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs046 Languages |
| `cbps_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs039 Signals |
| `cbtp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs029 Business Partner Types |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ccnt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `rtyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs040 Exchange Rate Types |
| `cinm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs055 Invoicing Methods |
| `cidm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs056 Invoice Delivery Methods |
| `mcod_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs058 Match Codes |
| `crat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs064 Credit Ratings |
| `ccra_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `ccic_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs009 Credit Insurance Companies |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `rpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `ccrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `rcrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `pfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `clmt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs205 Closing Methods |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `inpl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs245 Installment Plans |
| `bicy_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs054 Billing Cycles |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `cfcg_rcmp` | `integer` | `int` |  |  | CC: Reference Company of Financial Customer Group |
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

# tfgld482

LN tfgld482 Integration Transactions table, Generated 2026-04-10T19:41:45Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfgld482` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `guid`, `dbcr` |
| **Column count** | 92 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `guid` | `string` | `varchar` | `PK` | `not_null` | (required) GUID. Max length: 22 |
| `dbcr` | `integer` | `int` | `PK` | `not_null` | (required) Debit/Credit Indicator. Allowed values: 1, 2 |
| `dbcr_kw` | `string` | `varchar` |  |  | Debit/Credit Indicator (keyword). Allowed values: tfgld.dbcr.debit, tfgld.dbcr.credit |
| `ocmp` | `integer` | `int` |  |  | Business Object Company |
| `idtc` | `string` | `varchar` |  |  | Integration Document Type. Max length: 8 |
| `trdt` | `timestamp` | `timestamp_ntz` |  |  | Transaction Date |
| `tcmp` | `integer` | `int` |  |  | Business Object Company |
| `secd` | `string` | `varchar` |  |  | Sort Element. Max length: 12 |
| `seva` | `string` | `varchar` |  |  | Sort Element Value. Max length: 50 |
| `rbon` | `string` | `varchar` |  |  | Business Object. Max length: 17 |
| `rbid` | `string` | `varchar` |  |  | Business Object ID. Max length: 11 |
| `rpon` | `integer` | `int` |  |  | Position |
| `obre` | `string` | `varchar` |  |  | Business Object Reference. Max length: 90 |
| `buid` | `string` | `varchar` |  |  | Business Object Reference GUID. Max length: 22 |
| `bpid` | `string` | `varchar` |  |  | Business Partner. Max length: 9 |
| `ttyp` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `docn` | `integer` | `int` |  |  | Document Number |
| `btno` | `integer` | `int` |  |  | Batch |
| `lino` | `integer` | `int` |  |  | Line |
| `sint` | `integer` | `int` |  |  | Transaction Status. Allowed values: 1, 2, 3, 4, 5, 6 |
| `sint_kw` | `string` | `varchar` |  |  | Transaction Status (keyword). Allowed values: tfgld.ints.logged, tfgld.ints.mapped, tfgld.ints.posted, tfgld.ints.err.log, tfgld.ints.err.map, tfgld.ints.err.post |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `crus` | `string` | `varchar` |  |  | User (created). Max length: 16 |
| `usgr` | `string` | `varchar` |  |  | Financial User Group. Max length: 6 |
| `pous` | `string` | `varchar` |  |  | User (posted). Max length: 16 |
| `podt` | `timestamp` | `timestamp_ntz` |  |  | Posting Date |
| `prin` | `integer` | `int` |  |  | Printed. Allowed values: 1, 2 |
| `prin_kw` | `string` | `varchar` |  |  | Printed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `glcu` | `integer` | `int` |  |  | GL Code Used. Allowed values: 1, 2 |
| `glcu_kw` | `string` | `varchar` |  |  | GL Code Used (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `alam` | `integer` | `int` |  |  | Allow Ledger Mapping. Allowed values: 1, 2 |
| `alam_kw` | `string` | `varchar` |  |  | Allow Ledger Mapping (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `admm` | `integer` | `int` |  |  | Allow Dimension Mapping. Allowed values: 1, 2 |
| `admm_kw` | `string` | `varchar` |  |  | Allow Dimension Mapping (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fcom` | `integer` | `int` |  |  | Financial Company |
| `leac` | `string` | `varchar` |  |  | Ledger Account. Max length: 12 |
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
| `amnt` | `float` | `float` |  |  | Transaction Amount |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Currency Rate Date |
| `rtyp` | `string` | `varchar` |  |  | Currency Rate Type. Max length: 3 |
| `rate_1` | `float` | `float` |  |  | Currency Rate |
| `rate_2` | `float` | `float` |  |  | Currency Rate |
| `rate_3` | `float` | `float` |  |  | Currency Rate |
| `ratf_1` | `integer` | `int` |  |  | Currency Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Currency Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Currency Rate Factor |
| `eibc_1` | `integer` | `int` |  |  | Express in Base Currency. Allowed values: 0, 1, 2 |
| `eibc_kw_1` | `string` | `varchar` |  |  | Express in Base Currency (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `eibc_2` | `integer` | `int` |  |  | Express in Base Currency. Allowed values: 0, 1, 2 |
| `eibc_kw_2` | `string` | `varchar` |  |  | Express in Base Currency (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `eibc_3` | `integer` | `int` |  |  | Express in Base Currency. Allowed values: 0, 1, 2 |
| `eibc_kw_3` | `string` | `varchar` |  |  | Express in Base Currency (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `amth_1` | `float` | `float` |  |  | Amount in Home Currency |
| `amth_2` | `float` | `float` |  |  | Amount in Home Currency |
| `amth_3` | `float` | `float` |  |  | Amount in Home Currency |
| `cuni` | `string` | `varchar` |  |  | Unit. Max length: 3 |
| `nuni` | `float` | `float` |  |  | Quantity |
| `fyer` | `integer` | `int` |  |  | Financial Year |
| `fprd` | `integer` | `int` |  |  | Financial Period |
| `ryer` | `integer` | `int` |  |  | Reporting Year |
| `rprd` | `integer` | `int` |  |  | Reporting Period |
| `tyer` | `integer` | `int` |  |  | Tax Year |
| `tprd` | `integer` | `int` |  |  | Tax Period |
| `mscd` | `string` | `varchar` |  |  | Used Mapping Scheme Code. Max length: 6 |
| `msvs` | `integer` | `int` |  |  | Used Mapping Scheme Version |
| `idtc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld457 Integration Document Types |
| `rbon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld454 Business Objects |
| `usgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld435 Integration User Groups |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
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

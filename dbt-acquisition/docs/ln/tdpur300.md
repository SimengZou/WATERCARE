# tdpur300

LN tdpur300 Purchase Contracts table, Generated 2026-04-10T19:41:20Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdpur300` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cono` |
| **Column count** | 119 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cono` | `string` | `varchar` | `PK` | `not_null` | (required) Contract. Max length: 9 |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `otad` | `string` | `varchar` |  |  | Buy-from Address. Max length: 9 |
| `otcn` | `string` | `varchar` |  |  | Buy-from Contact. Max length: 9 |
| `sfbp` | `string` | `varchar` |  |  | Ship-from Business Partner. Max length: 9 |
| `sfad` | `string` | `varchar` |  |  | Ship-from Address. Max length: 9 |
| `sfcn` | `string` | `varchar` |  |  | Ship-from Contact. Max length: 9 |
| `ifbp` | `string` | `varchar` |  |  | Invoice-from Business Partner. Max length: 9 |
| `ifad` | `string` | `varchar` |  |  | Invoice-from Address. Max length: 9 |
| `ifcn` | `string` | `varchar` |  |  | Invoice-from Contact. Max length: 9 |
| `ptbp` | `string` | `varchar` |  |  | Pay-to Business Partner. Max length: 9 |
| `ptad` | `string` | `varchar` |  |  | Pay-to Address. Max length: 9 |
| `ptcn` | `string` | `varchar` |  |  | Pay-to Contact. Max length: 9 |
| `cdes__en_us` | `string` | `varchar` |  | `not_null` | (required) Contract Description - base language. Max length: 30 |
| `icyp` | `integer` | `int` |  |  | Contract Type. Allowed values: 1, 2 |
| `icyp_kw` | `string` | `varchar` |  |  | Contract Type (keyword). Allowed values: tcicyp.year, tcicyp.proj |
| `icap` | `integer` | `int` |  |  | Contract Status. Allowed values: 1, 2, 3 |
| `icap_kw` | `string` | `varchar` |  |  | Contract Status (keyword). Allowed values: tcicap.free, tcicap.active, tcicap.closed |
| `icpr` | `integer` | `int` |  |  | Contract Acknowledgement. Allowed values: 1, 2 |
| `icpr_kw` | `string` | `varchar` |  |  | Contract Acknowledgement (keyword). Allowed values: tcicpr.print, tcicpr.dont.print |
| `csts` | `integer` | `int` |  |  | Print Status. Allowed values: 1, 2, 3, 4, 5, 6 |
| `csts_kw` | `string` | `varchar` |  |  | Print Status (keyword). Allowed values: tccsts.printed, tccsts.not.printed, tccsts.changed, tccsts.draft, tccsts.original, tccsts.changes.printed |
| `lcmp` | `integer` | `int` |  |  | Logistic Company |
| `cofc` | `string` | `varchar` |  |  | Purchase Office. Max length: 6 |
| `rcmp` | `integer` | `int` |  |  | Responsible Office Logistic Company |
| `rofc` | `string` | `varchar` |  |  | Responsible Office. Max length: 6 |
| `refe__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference A - base language. Max length: 30 |
| `refb__en_us` | `string` | `varchar` |  | `not_null` | (required) Reference B - base language. Max length: 30 |
| `cdat` | `timestamp` | `timestamp_ntz` |  |  | Contract Date |
| `sdat` | `timestamp` | `timestamp_ntz` |  |  | Effective Date |
| `edat` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `rcsi` | `string` | `varchar` |  |  | Receiving Site. Max length: 9 |
| `cwar` | `string` | `varchar` |  |  | Warehouse. Max length: 6 |
| `cadr` | `string` | `varchar` |  |  | Receipt Address. Max length: 9 |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `ccon` | `string` | `varchar` |  |  | Buyer. Max length: 9 |
| `bpcl` | `string` | `varchar` |  |  | Tax Classification. Max length: 12 |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `cfrw` | `string` | `varchar` |  |  | Carrier/LSP. Max length: 3 |
| `cdec` | `string` | `varchar` |  |  | Delivery Terms. Max length: 3 |
| `ptpa` | `string` | `varchar` |  |  | Point of Title Passage. Max length: 9 |
| `conf__en_us` | `string` | `varchar` |  | `not_null` | (required) Confirmation Number - base language. Max length: 30 |
| `cotp` | `string` | `varchar` |  |  | Purchase Order Type. Max length: 3 |
| `nope` | `integer` | `int` |  |  | Notice Period |
| `ocon` | `string` | `varchar` |  |  | Original Contract. Max length: 9 |
| `trid` | `string` | `varchar` |  |  | Terms and Conditions ID. Max length: 9 |
| `chrq` | `integer` | `int` |  |  | Change Request. Allowed values: 1, 2, 3 |
| `chrq_kw` | `string` | `varchar` |  |  | Change Request (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `revn` | `integer` | `int` |  |  | Revision |
| `crby` | `string` | `varchar` |  |  | Change Request Created by. Max length: 16 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Change Request Creation Date |
| `srcn` | `string` | `varchar` |  |  | Original Purchase Contract. Max length: 9 |
| `crst` | `integer` | `int` |  |  | Change Request Status. Allowed values: 5, 10, 15, 20, 25, 28, 30, 35, 40, 45, 50 |
| `crst_kw` | `string` | `varchar` |  |  | Change Request Status (keyword). Allowed values: tdpur.hdst.created, tdpur.hdst.approved, tdpur.hdst.sent, tdpur.hdst.in.process, tdpur.hdst.closed, tdpur.hdst.canc.in.proc, tdpur.hdst.cancelled, tdpur.hdst.modified, tdpur.hdst.blocked, tdpur.hdst.released, tdpur.hdst.not.applicable |
| `crrq` | `integer` | `int` |  |  | Change Request Required. Allowed values: 1, 2 |
| `crrq_kw` | `string` | `varchar` |  |  | Change Request Required (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `crin` | `integer` | `int` |  |  | Internal Change Request. Allowed values: 1, 2 |
| `crin_kw` | `string` | `varchar` |  |  | Internal Change Request (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `copc` | `integer` | `int` |  |  | Corporate Purchase Contract. Allowed values: 1, 2 |
| `copc_kw` | `string` | `varchar` |  |  | Corporate Purchase Contract (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccty` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `cvyn` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `cvyn_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `txta` | `integer` | `int` |  |  | Header Text |
| `txtb` | `integer` | `int` |  |  | Footer Text |
| `txtt` | `integer` | `int` |  |  | Termination text |
| `crht` | `integer` | `int` |  |  | Change Request Text |
| `cdf_aval` | `float` | `float` |  |  | Award Value |
| `cdf_camt` | `float` | `float` |  |  | Contract Amount |
| `cdf_comt` | `float` | `float` |  |  | Consumed Amount |
| `cdf_conn` | `string` | `varchar` |  |  | Contract Name. Max length: 100 |
| `cdf_pwno` | `string` | `varchar` |  |  | ProjectWise Number. Max length: 10 |
| `cdf_ramt` | `float` | `float` |  |  | Remaining Amount |
| `cdf_vval` | `float` | `float` |  |  | Variation Value |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `otad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `otcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `sfbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom121 Ship-from Business Partners |
| `sfad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `sfcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ifbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ifad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ifcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `ptbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ptad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ptcn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom140 Contacts |
| `lcmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `cofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur012 Purchase Offices |
| `rcmp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `rofc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur012 Purchase Offices |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `rcsi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `cadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom001 Employees - General |
| `bpcl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax016 Tax Classifications |
| `ccrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `cfrw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `cdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `ptpa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs042 Points of Title Passage |
| `cotp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur094 Purchase Order Types |
| `ocon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tdpur300 Purchase Contracts |
| `trid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctrm100 Terms and Conditions |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtt_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `crht_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
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

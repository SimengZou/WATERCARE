# whinh440

LN whinh440 Loads table, Generated 2026-04-10T19:42:50Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whinh440` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `load` |
| **Column count** | 94 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `load` | `string` | `varchar` | `PK` | `not_null` | (required) Load. Max length: 9 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `crte` | `string` | `varchar` |  |  | Route. Max length: 5 |
| `pddt` | `timestamp` | `timestamp_ntz` |  |  | Planned Delivery Date |
| `carp` | `string` | `varchar` |  |  | Pick-up Carrier/LSP. Max length: 3 |
| `trmp` | `integer` | `int` |  |  | Pick-up Transport Category. Allowed values: 10, 15, 20, 25, 30, 35, 40, 45, 50, 52, 54, 56, 58, 60, 62, 70, 80, 83, 85, 87, 90, 93, 96, 98, 100 |
| `trmp_kw` | `string` | `varchar` |  |  | Pick-up Transport Category (keyword). Allowed values: tccom.trmd.water, tccom.trmd.water.cont, tccom.trmd.rail, tccom.trmd.rail.cont, tccom.trmd.road, tccom.trmd.road.cont, tccom.trmd.air, tccom.trmd.air.charter, tccom.trmd.post, tccom.trmd.contract.carr, tccom.trmd.customer.pickup, tccom.trmd.truckload, tccom.trmd.mail, tccom.trmd.intermodal, tccom.trmd.consolidation, tccom.trmd.instal, tccom.trmd.inlwater, tccom.trmd.express.air, tccom.trmd.express.truck, tccom.trmd.express.rail, tccom.trmd.own, tccom.trmd.pool.point, tccom.trmd.milk.run, tccom.trmd.piggy.back, tccom.trmd.none |
| `tmgp` | `string` | `varchar` |  |  | Pick-up Transport Means Group. Max length: 3 |
| `tmcp` | `string` | `varchar` |  |  | Pick-up Transport Means Combination. Max length: 6 |
| `trip__en_us` | `string` | `varchar` |  | `not_null` | (required) Pick-up Means of Transport - base language. Max length: 30 |
| `card` | `string` | `varchar` |  |  | Delivery Carrier/LSP. Max length: 3 |
| `tmgd` | `string` | `varchar` |  |  | Delivery Transport Means Group. Max length: 3 |
| `tmcd` | `string` | `varchar` |  |  | Delivery Transport Means Combination. Max length: 6 |
| `trid__en_us` | `string` | `varchar` |  | `not_null` | (required) Delivery Means of Transport - base language. Max length: 30 |
| `wght` | `float` | `float` |  |  | Total Weight |
| `cuni` | `string` | `varchar` |  |  | Weight Unit. Max length: 3 |
| `fxwt` | `integer` | `int` |  |  | Fixed Weight. Allowed values: 1, 2 |
| `fxwt_kw` | `string` | `varchar` |  |  | Fixed Weight (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mwgt` | `float` | `float` |  |  | Maximum Weight |
| `shst` | `integer` | `int` |  |  | Status. Allowed values: 1, 2, 3, 10, 20, 30, 40 |
| `shst_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: whinh.shst.open, whinh.shst.frozen, whinh.shst.confirmed, whinh.shst.part.frozen, whinh.shst.confirming, whinh.shst.projected, whinh.shst.canceled |
| `cpro__en_us` | `string` | `varchar` |  | `not_null` | (required) Carrier Tracking Number - base language. Max length: 30 |
| `ctnt` | `integer` | `int` |  |  | Carrier Tracking Number Type. Allowed values: 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 77, 78, 80, 85 |
| `ctnt_kw` | `string` | `varchar` |  |  | Carrier Tracking Number Type (keyword). Allowed values: tctntp.pro, tctntp.h.airway.bill, tctntp.m.airway.bill, tctntp.booking, tctntp.h.ocean.b.of.l, tctntp.m.ocean.b.of.l, tctntp.equipment, tctntp.seal, tctntp.license.plate, tctntp.bordero, tctntp.general.cargo, tctntp.parcel, tctntp.airbill, tctntp.vessel.name, tctntp.express.goods, tctntp.aetc, tctntp.freight.bill, tctntp.undefined, tctntp.not.appl |
| `trnr__en_us` | `string` | `varchar` |  | `not_null` | (required) Tracking Number - base language. Max length: 30 |
| `tntp` | `integer` | `int` |  |  | Tracking Number Type. Allowed values: 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 77, 78, 80, 85 |
| `tntp_kw` | `string` | `varchar` |  |  | Tracking Number Type (keyword). Allowed values: tctntp.pro, tctntp.h.airway.bill, tctntp.m.airway.bill, tctntp.booking, tctntp.h.ocean.b.of.l, tctntp.m.ocean.b.of.l, tctntp.equipment, tctntp.seal, tctntp.license.plate, tctntp.bordero, tctntp.general.cargo, tctntp.parcel, tctntp.airbill, tctntp.vessel.name, tctntp.express.goods, tctntp.aetc, tctntp.freight.bill, tctntp.undefined, tctntp.not.appl |
| `trna__en_us` | `string` | `varchar` |  | `not_null` | (required) Tracking Number 1 - base language. Max length: 30 |
| `tnta` | `integer` | `int` |  |  | Tracking Number 1 Type. Allowed values: 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 77, 78, 80, 85 |
| `tnta_kw` | `string` | `varchar` |  |  | Tracking Number 1 Type (keyword). Allowed values: tctntp.pro, tctntp.h.airway.bill, tctntp.m.airway.bill, tctntp.booking, tctntp.h.ocean.b.of.l, tctntp.m.ocean.b.of.l, tctntp.equipment, tctntp.seal, tctntp.license.plate, tctntp.bordero, tctntp.general.cargo, tctntp.parcel, tctntp.airbill, tctntp.vessel.name, tctntp.express.goods, tctntp.aetc, tctntp.freight.bill, tctntp.undefined, tctntp.not.appl |
| `trnb__en_us` | `string` | `varchar` |  | `not_null` | (required) Tracking Number 2 - base language. Max length: 30 |
| `tntb` | `integer` | `int` |  |  | Tracking Number 2 Type. Allowed values: 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 77, 78, 80, 85 |
| `tntb_kw` | `string` | `varchar` |  |  | Tracking Number 2 Type (keyword). Allowed values: tctntp.pro, tctntp.h.airway.bill, tctntp.m.airway.bill, tctntp.booking, tctntp.h.ocean.b.of.l, tctntp.m.ocean.b.of.l, tctntp.equipment, tctntp.seal, tctntp.license.plate, tctntp.bordero, tctntp.general.cargo, tctntp.parcel, tctntp.airbill, tctntp.vessel.name, tctntp.express.goods, tctntp.aetc, tctntp.freight.bill, tctntp.undefined, tctntp.not.appl |
| `stcp` | `integer` | `int` |  |  | Ship-to Company |
| `stty` | `integer` | `int` |  |  | Ship-to Type. Allowed values: 1, 2, 3, 4, 10 |
| `stty_kw` | `string` | `varchar` |  |  | Ship-to Type (keyword). Allowed values: tctyps.warehouse, tctyps.partner, tctyps.project, tctyps.work.center, tctyps.not.appl |
| `stco` | `string` | `varchar` |  |  | Ship-to Code. Max length: 9 |
| `sans` | `integer` | `int` |  |  | Still Allow New Shipments. Allowed values: 1, 2 |
| `sans_kw` | `string` | `varchar` |  |  | Still Allow New Shipments (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aetc` | `integer` | `int` |  |  | Authorize Excess Transportation Costs. Allowed values: 1, 2 |
| `aetc_kw` | `string` | `varchar` |  |  | Authorize Excess Transportation Costs (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `etrn` | `string` | `varchar` |  |  | Excess Transportation Reason. Max length: 6 |
| `aetr` | `integer` | `int` |  |  | Excess Transportation Responsibility. Allowed values: 10, 20, 30, 40, 50, 60, 100 |
| `aetr_kw` | `string` | `varchar` |  |  | Excess Transportation Responsibility (keyword). Allowed values: tcmcs.aetr.cust.plant, tcmcs.aetr.mat.issuer, tcmcs.aetr.supplier.auth, tcmcs.aetr.carrier, tcmcs.aetr.to.be.determine, tcmcs.aetr.mutually.def, tcmcs.aetr.not.appl |
| `trpc__en_us` | `string` | `varchar` |  | `not_null` | (required) Obsolete - base language. Max length: 12 |
| `trpa__en_us` | `string` | `varchar` |  | `not_null` | (required) Obsolete - base language. Max length: 30 |
| `sspl` | `integer` | `int` |  |  | Single Shipment per Load. Allowed values: 1, 2 |
| `sspl_kw` | `string` | `varchar` |  |  | Single Shipment per Load (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `eamt` | `float` | `float` |  |  | Estimated Freight Costs |
| `eacu` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `psts` | `integer` | `int` |  |  | Publishing Status. Allowed values: 10, 20, 30, 32, 34, 36, 40 |
| `psts_kw` | `string` | `varchar` |  |  | Publishing Status (keyword). Allowed values: whinh.spst.to.be.published, whinh.spst.published, whinh.spst.modified, whinh.spst.validating, whinh.spst.val.error, whinh.spst.validated, whinh.spst.not.applicable |
| `imcs` | `string` | `varchar` |  |  | Intermediate Consignee. Max length: 35 |
| `sqdp` | `integer` | `int` |  |  | Sequencing during Picking. Allowed values: 10, 20, 30 |
| `sqdp_kw` | `string` | `varchar` |  |  | Sequencing during Picking (keyword). Allowed values: whinh.asds.ascending, whinh.asds.descending, whinh.asds.not.applicable |
| `sqdl` | `integer` | `int` |  |  | Sequencing during Loading. Allowed values: 10, 20, 30 |
| `sqdl_kw` | `string` | `varchar` |  |  | Sequencing during Loading (keyword). Allowed values: whinh.asds.ascending, whinh.asds.descending, whinh.asds.not.applicable |
| `tcap` | `integer` | `int` |  |  | Localized Authorization Procedure Applicable. Allowed values: 1, 2 |
| `tcap_kw` | `string` | `varchar` |  |  | Localized Authorization Procedure Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tccp` | `integer` | `int` |  |  | Localized Authorization Procedure Complete. Allowed values: 1, 2 |
| `tccp_kw` | `string` | `varchar` |  |  | Localized Authorization Procedure Complete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ptro` | `integer` | `int` |  |  | Publish Transport Order. Allowed values: 1, 2 |
| `ptro_kw` | `string` | `varchar` |  |  | Publish Transport Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tops` | `integer` | `int` |  |  | Transport Order Publishing Status. Allowed values: 10, 20, 30, 40, 50 |
| `tops_kw` | `string` | `varchar` |  |  | Transport Order Publishing Status (keyword). Allowed values: whinh.tops.to.be.published, whinh.tops.published, whinh.tops.modified, whinh.tops.cancelled, whinh.tops.not.applicable |
| `todd` | `timestamp` | `timestamp_ntz` |  |  | Transport Order Due Date |
| `tolt` | `integer` | `int` |  |  | Transport Order Due Lead Time |
| `totu` | `integer` | `int` |  |  | Transport Order Due Lead Time Unit. Allowed values: 0, 5, 10 |
| `totu_kw` | `string` | `varchar` |  |  | Transport Order Due Lead Time Unit (keyword). Allowed values: , tctope.hours, tctope.days |
| `mrno__en_us` | `string` | `varchar` |  | `not_null` | (required) Movement Reference Number - base language. Max length: 30 |
| `mrdt` | `timestamp` | `timestamp_ntz` |  |  | Movement Reference Date |
| `mrtp` | `integer` | `int` |  |  | Movement Type. Allowed values: 10, 20, 30, 255 |
| `mrtp_kw` | `string` | `varchar` |  |  | Movement Type (keyword). Allowed values: whinh.movetype.eu.and.uk, whinh.movetype.non.eu.and.uk, whinh.movetype.bonded, whinh.movetype.not.appl |
| `exfr` | `integer` | `int` |  |  | Expedited. Allowed values: 1, 2 |
| `exfr_kw` | `string` | `varchar` |  |  | Expedited (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `text` | `integer` | `int` |  |  | Text |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `crte_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs004 Routes |
| `carp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `card_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs080 Carriers/LSP |
| `cuni_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `etrn_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `eacu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
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

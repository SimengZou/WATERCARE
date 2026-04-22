# tdsls094

LN tdsls094 Sales Order Types table, Generated 2026-04-10T19:41:23Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tdsls094` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `sotp` |
| **Column count** | 88 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `sotp` | `string` | `varchar` | `PK` | `not_null` | (required) Sales Order Type. Max length: 3 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `reto` | `integer` | `int` |  |  | Return Order. Allowed values: 1, 2, 3 |
| `reto_kw` | `string` | `varchar` |  |  | Return Order (keyword). Allowed values: tdpur.reto.yes, tdpur.reto.no, tdpur.reto.rejectgoods |
| `coun` | `integer` | `int` |  |  | Collect Order. Allowed values: 1, 2 |
| `coun_kw` | `string` | `varchar` |  |  | Collect Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `retb` | `integer` | `int` |  |  | Retro-Billed. Allowed values: 1, 2 |
| `retb_kw` | `string` | `varchar` |  |  | Retro-Billed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sund` | `integer` | `int` |  |  | Cost Order. Allowed values: 1, 2 |
| `sund_kw` | `string` | `varchar` |  |  | Cost Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cnsi` | `integer` | `int` |  |  | Consignment Invoicing. Allowed values: 1, 2 |
| `cnsi_kw` | `string` | `varchar` |  |  | Consignment Invoicing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cnsr` | `integer` | `int` |  |  | Consignment Replenishment. Allowed values: 1, 2 |
| `cnsr_kw` | `string` | `varchar` |  |  | Consignment Replenishment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wrhp` | `string` | `varchar` |  |  | Warehousing Order Type. Max length: 3 |
| `cphl` | `integer` | `int` |  |  | Component Handling. Allowed values: 5, 10, 15 |
| `cphl_kw` | `string` | `varchar` |  |  | Component Handling (keyword). Allowed values: tdcphl.not.applicable, tdcphl.sales.bom, tdcphl.component.lines |
| `scon` | `integer` | `int` |  |  | Shipping Constraint. Allowed values: 1, 2, 3, 4, 5, 6, 10 |
| `scon_kw` | `string` | `varchar` |  |  | Shipping Constraint (keyword). Allowed values: tdscon.none, tdscon.sc, tdscon.rc, tdscon.oc, tdscon.sca, tdscon.kc, tdscon.not.applicable |
| `blcs` | `string` | `varchar` |  |  | Block As of Activity. Max length: 13 |
| `crdc` | `integer` | `int` |  |  | Credit Check. Allowed values: 1, 2 |
| `crdc_kw` | `string` | `varchar` |  |  | Credit Check (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `gmrc` | `integer` | `int` |  |  | Gross Margin Control. Allowed values: 1, 2 |
| `gmrc_kw` | `string` | `varchar` |  |  | Gross Margin Control (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `apur` | `integer` | `int` |  |  | Allow Purchase Orders. Allowed values: 1, 2 |
| `apur_kw` | `string` | `varchar` |  |  | Allow Purchase Orders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lcpl` | `integer` | `int` |  |  | Direct Link Cross-docking. Allowed values: 1, 2 |
| `lcpl_kw` | `string` | `varchar` |  |  | Direct Link Cross-docking (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `upls` | `integer` | `int` |  |  | Update Lost Sales. Allowed values: 1, 2 |
| `upls_kw` | `string` | `varchar` |  |  | Update Lost Sales (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `upsh` | `integer` | `int` |  |  | Update Demand Sales. Allowed values: 1, 2 |
| `upsh_kw` | `string` | `varchar` |  |  | Update Demand Sales (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aprd` | `integer` | `int` |  |  | Allow Production Orders. Allowed values: 1, 2 |
| `aprd_kw` | `string` | `varchar` |  |  | Allow Production Orders (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mwdl` | `integer` | `int` |  |  | Manual Activities Awaiting Delivery. Allowed values: 1, 2 |
| `mwdl_kw` | `string` | `varchar` |  |  | Manual Activities Awaiting Delivery (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aaps` | `integer` | `int` |  |  | Automatically Approve Sales Order. Allowed values: 1, 2 |
| `aaps_kw` | `string` | `varchar` |  |  | Automatically Approve Sales Order (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ngso` | `string` | `varchar` |  |  | Order Group. Max length: 3 |
| `seso` | `string` | `varchar` |  |  | Order Series. Max length: 8 |
| `ngsq` | `string` | `varchar` |  |  | Quotation Group. Max length: 3 |
| `sesq` | `string` | `varchar` |  |  | Quotation Series. Max length: 8 |
| `ngsc` | `string` | `varchar` |  |  | Contract Group. Max length: 3 |
| `sesc` | `string` | `varchar` |  |  | Contract Series. Max length: 8 |
| `einc` | `integer` | `int` |  |  | Extended Inventory Check. Allowed values: 1, 2 |
| `einc_kw` | `string` | `varchar` |  |  | Extended Inventory Check (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `incm` | `integer` | `int` |  |  | Inventory Check Moment. Allowed values: 1, 5, 10, 25 |
| `incm_kw` | `string` | `varchar` |  |  | Inventory Check Moment (keyword). Allowed values: tdsls.incm.never, tdsls.incm.manual.entry, tdsls.incm.batch, tdsls.incm.not.applicable |
| `wpal` | `integer` | `int` |  |  | Write Planned Inventory Transactions only for Accepted Lines. Allowed values: 1, 2 |
| `wpal_kw` | `string` | `varchar` |  |  | Write Planned Inventory Transactions only for Accepted Lines (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ssoa` | `integer` | `int` |  |  | Non-generic/Not Planned Item Option. Allowed values: 1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57, 61, 65, 69, 73, 77, 79, 81, 85, 99 |
| `ssoa_kw` | `string` | `varchar` |  |  | Non-generic/Not Planned Item Option (keyword). Allowed values: tdsls.ssop.channel.plan, tdsls.ssop.item.mastr.plan, tdsls.ssop.item.ordr.plan, tdsls.ssop.planned.inv, tdsls.ssop.inv.item.whs, tdsls.ssop.inv.specific, tdsls.ssop.inv.commitments, tdsls.ssop.alloc.buffer, tdsls.ssop.alt.items, tdsls.ssop.pur.advice, tdsls.ssop.dir.del, tdsls.ssop.purchase, tdsls.ssop.production, tdsls.ssop.transfer, tdsls.ssop.atp, tdsls.ssop.atp.fixed.date, tdsls.ssop.atp.fixed.wh, tdsls.ssop.adjust.qty, tdsls.ssop.adj.hold.back, tdsls.ssop.del.date, tdsls.ssop.force.accept, tdsls.ssop.restore.old, tdsls.ssop.no.action, tdsls.ssop.none |
| `ssob` | `integer` | `int` |  |  | Non-generic/Planned Item Option. Allowed values: 1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57, 61, 65, 69, 73, 77, 79, 81, 85, 99 |
| `ssob_kw` | `string` | `varchar` |  |  | Non-generic/Planned Item Option (keyword). Allowed values: tdsls.ssop.channel.plan, tdsls.ssop.item.mastr.plan, tdsls.ssop.item.ordr.plan, tdsls.ssop.planned.inv, tdsls.ssop.inv.item.whs, tdsls.ssop.inv.specific, tdsls.ssop.inv.commitments, tdsls.ssop.alloc.buffer, tdsls.ssop.alt.items, tdsls.ssop.pur.advice, tdsls.ssop.dir.del, tdsls.ssop.purchase, tdsls.ssop.production, tdsls.ssop.transfer, tdsls.ssop.atp, tdsls.ssop.atp.fixed.date, tdsls.ssop.atp.fixed.wh, tdsls.ssop.adjust.qty, tdsls.ssop.adj.hold.back, tdsls.ssop.del.date, tdsls.ssop.force.accept, tdsls.ssop.restore.old, tdsls.ssop.no.action, tdsls.ssop.none |
| `ssoc` | `integer` | `int` |  |  | Generic/Not Planned Item Option. Allowed values: 1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57, 61, 65, 69, 73, 77, 79, 81, 85, 99 |
| `ssoc_kw` | `string` | `varchar` |  |  | Generic/Not Planned Item Option (keyword). Allowed values: tdsls.ssop.channel.plan, tdsls.ssop.item.mastr.plan, tdsls.ssop.item.ordr.plan, tdsls.ssop.planned.inv, tdsls.ssop.inv.item.whs, tdsls.ssop.inv.specific, tdsls.ssop.inv.commitments, tdsls.ssop.alloc.buffer, tdsls.ssop.alt.items, tdsls.ssop.pur.advice, tdsls.ssop.dir.del, tdsls.ssop.purchase, tdsls.ssop.production, tdsls.ssop.transfer, tdsls.ssop.atp, tdsls.ssop.atp.fixed.date, tdsls.ssop.atp.fixed.wh, tdsls.ssop.adjust.qty, tdsls.ssop.adj.hold.back, tdsls.ssop.del.date, tdsls.ssop.force.accept, tdsls.ssop.restore.old, tdsls.ssop.no.action, tdsls.ssop.none |
| `ssod` | `integer` | `int` |  |  | Generic/Planned Item Option. Allowed values: 1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57, 61, 65, 69, 73, 77, 79, 81, 85, 99 |
| `ssod_kw` | `string` | `varchar` |  |  | Generic/Planned Item Option (keyword). Allowed values: tdsls.ssop.channel.plan, tdsls.ssop.item.mastr.plan, tdsls.ssop.item.ordr.plan, tdsls.ssop.planned.inv, tdsls.ssop.inv.item.whs, tdsls.ssop.inv.specific, tdsls.ssop.inv.commitments, tdsls.ssop.alloc.buffer, tdsls.ssop.alt.items, tdsls.ssop.pur.advice, tdsls.ssop.dir.del, tdsls.ssop.purchase, tdsls.ssop.production, tdsls.ssop.transfer, tdsls.ssop.atp, tdsls.ssop.atp.fixed.date, tdsls.ssop.atp.fixed.wh, tdsls.ssop.adjust.qty, tdsls.ssop.adj.hold.back, tdsls.ssop.del.date, tdsls.ssop.force.accept, tdsls.ssop.restore.old, tdsls.ssop.no.action, tdsls.ssop.none |
| `ssoe` | `integer` | `int` |  |  | Inventory Shortage Recheck Option for Unplanned Item. Allowed values: 1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57, 61, 65, 69, 73, 77, 79, 81, 85, 99 |
| `ssoe_kw` | `string` | `varchar` |  |  | Inventory Shortage Recheck Option for Unplanned Item (keyword). Allowed values: tdsls.ssop.channel.plan, tdsls.ssop.item.mastr.plan, tdsls.ssop.item.ordr.plan, tdsls.ssop.planned.inv, tdsls.ssop.inv.item.whs, tdsls.ssop.inv.specific, tdsls.ssop.inv.commitments, tdsls.ssop.alloc.buffer, tdsls.ssop.alt.items, tdsls.ssop.pur.advice, tdsls.ssop.dir.del, tdsls.ssop.purchase, tdsls.ssop.production, tdsls.ssop.transfer, tdsls.ssop.atp, tdsls.ssop.atp.fixed.date, tdsls.ssop.atp.fixed.wh, tdsls.ssop.adjust.qty, tdsls.ssop.adj.hold.back, tdsls.ssop.del.date, tdsls.ssop.force.accept, tdsls.ssop.restore.old, tdsls.ssop.no.action, tdsls.ssop.none |
| `ssof` | `integer` | `int` |  |  | Inventory Shortage Recheck Option for Planned Item. Allowed values: 1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53, 57, 61, 65, 69, 73, 77, 79, 81, 85, 99 |
| `ssof_kw` | `string` | `varchar` |  |  | Inventory Shortage Recheck Option for Planned Item (keyword). Allowed values: tdsls.ssop.channel.plan, tdsls.ssop.item.mastr.plan, tdsls.ssop.item.ordr.plan, tdsls.ssop.planned.inv, tdsls.ssop.inv.item.whs, tdsls.ssop.inv.specific, tdsls.ssop.inv.commitments, tdsls.ssop.alloc.buffer, tdsls.ssop.alt.items, tdsls.ssop.pur.advice, tdsls.ssop.dir.del, tdsls.ssop.purchase, tdsls.ssop.production, tdsls.ssop.transfer, tdsls.ssop.atp, tdsls.ssop.atp.fixed.date, tdsls.ssop.atp.fixed.wh, tdsls.ssop.adjust.qty, tdsls.ssop.adj.hold.back, tdsls.ssop.del.date, tdsls.ssop.force.accept, tdsls.ssop.restore.old, tdsls.ssop.no.action, tdsls.ssop.none |
| `udps` | `integer` | `int` |  |  | Use Delivery Patterns. Allowed values: 1, 2 |
| `udps_kw` | `string` | `varchar` |  |  | Use Delivery Patterns (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sdec` | `string` | `varchar` |  |  | Standard Delivery Terms. Max length: 3 |
| `spay` | `string` | `varchar` |  |  | Standard Payment Terms. Max length: 3 |
| `efdt` | `timestamp` | `timestamp_ntz` |  |  | Effective Date |
| `exdt` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `pmsk` | `string` | `varchar` |  |  | Obsolete. Max length: 20 |
| `proc` | `string` | `varchar` |  |  | Obsolete. Max length: 6 |
| `ngso_seso_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngso_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngsq_sesq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngsq_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `ngsc_sesc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngsc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
| `sdec_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs041 Delivery Terms |
| `spay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
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

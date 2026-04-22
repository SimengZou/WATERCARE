# tcmcs005

LN tcmcs005 Reasons table, Generated 2026-04-10T19:41:09Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tcmcs005` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cdis` |
| **Column count** | 24 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cdis` | `string` | `varchar` | `PK` | `not_null` | (required) Reason. Max length: 6 |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 30 |
| `rstp` | `integer` | `int` |  |  | Reason Type. Allowed values: 1, 10, 11, 12, 13, 15, 18, 20, 25, 27, 29, 30, 35, 40, 41, 42, 43, 45, 50, 55, 60, 65, 66, 67, 68, 69, 70, 75, 77, 80, 81, 82, 83, 85, 90, 92, 93, 95, 96, 98, 99, 100, 101, 102, 103, 104, 105, 107, 110, 112, 115, 120, 125, 126, 130, 131, 132, 134, 135, 137, 138, 140, 150, 160, 170, 175, 180, 185, 186, 187, 190, 191 |
| `rstp_kw` | `string` | `varchar` |  |  | Reason Type (keyword). Allowed values: tcmcs.rstp.not.avail, tcmcs.rstp.rej.goods, tcmcs.rstp.ret.goods, tcmcs.rstp.destroyed, tcmcs.rstp.damaged, tcmcs.rstp.hold.receipt, tcmcs.rstp.work.quot, tcmcs.rstp.rej.quot, tcmcs.rstp.rej.req, tcmcs.rstp.reject.prod, tcmcs.rstp.order.split, tcmcs.rstp.inc.shipment, tcmcs.rstp.pcs.closed, tcmcs.rstp.tax.exem, tcmcs.rstp.tax.article, tcmcs.rstp.cash.flow, tcmcs.rstp.invoicing, tcmcs.rstp.assignment.rej, tcmcs.rstp.inv.adjustment, tcmcs.rstp.inv.shortage, tcmcs.rstp.blocking, tcmcs.rstp.blocking.irp, tcmcs.rstp.rej.blocking, tcmcs.rstp.insp.blocking, tcmcs.rstp.inv.own.change, tcmcs.rstp.alloc.change, tcmcs.rstp.item.busp, tcmcs.rstp.block.man, tcmcs.rstp.block.mpn, tcmcs.rstp.cancel, tcmcs.rstp.interrupt, tcmcs.rstp.succes.fail, tcmcs.rstp.failure, tcmcs.rstp.mandate.amend, tcmcs.rstp.service.inspect, tcmcs.rstp.claim.approval, tcmcs.rstp.claim.rejection, tcmcs.rstp.scrap.tools, tcmcs.rstp.tool.adj, tcmcs.rstp.quality.mgt, tcmcs.rstp.non.conformance, tcmcs.rstp.undefined, tcmcs.rstp.destroy.reject, tcmcs.rstp.accept.reject, tcmcs.rstp.motive, tcmcs.rstp.del.code, tcmcs.rstp.load.plan, tcmcs.rstp.return.reject, tcmcs.rstp.finding, tcmcs.rstp.change, tcmcs.rstp.revaluation, tcmcs.rstp.ret.scrap, tcmcs.rstp.cycle.count, tcmcs.rstp.block.warehouse, tcmcs.rstp.budget.transfer, tcmcs.rstp.budget.adjust, tcmcs.rstp.budget.amend, tcmcs.rstp.peg.transfer, tcmcs.rstp.peg.audit.hist, tcmcs.rstp.rework.overiss, tcmcs.rstp.final.payback, tcmcs.rstp.disposition, tcmcs.rstp.rej.personnel, tcmcs.rstp.cust.auth, tcmcs.rstp.reuse.suppl.inv, tcmcs.rstp.close, tcmcs.rstp.lic.exem, tcmcs.rstp.absence, tcmcs.rstp.attendance, tcmcs.rstp.basic.shift.dif, tcmcs.rstp.tag.transfer, tcmcs.rstp.exception |
| `drpi` | `integer` | `int` |  |  | Direct Pay Indicator. Allowed values: 1, 2 |
| `drpi_kw` | `string` | `varchar` |  |  | Direct Pay Indicator (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `appr` | `integer` | `int` |  |  | Approval. Allowed values: 1, 2 |
| `appr_kw` | `string` | `varchar` |  |  | Approval (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bilb` | `integer` | `int` |  |  | Billable. Allowed values: 1, 2 |
| `bilb_kw` | `string` | `varchar` |  |  | Billable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `efdt` | `timestamp` | `timestamp_ntz` |  |  | Effective Date |
| `exdt` | `timestamp` | `timestamp_ntz` |  |  | Expiry Date |
| `etrt` | `integer` | `int` |  |  | Excess Transportation Reason. Allowed values: 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100, 105, 110, 115, 200 |
| `etrt_kw` | `string` | `varchar` |  |  | Excess Transportation Reason (keyword). Allowed values: tcmcs.etrt.spec.rail, tcmcs.etrt.eng.change, tcmcs.etrt.spec.error, tcmcs.etrt.shpm.tracing, tcmcs.etrt.inv.loss, tcmcs.etrt.build.sched, tcmcs.etrt.vendor.sched, tcmcs.etrt.fail.last.shpm, tcmcs.etrt.carrier.loss, tcmcs.etrt.trans.fail, tcmcs.etrt.ins.weight, tcmcs.etrt.reject, tcmcs.etrt.trans.delay, tcmcs.etrt.equipment, tcmcs.etrt.release.error, tcmcs.etrt.record.error, tcmcs.etrt.sched.incr, tcmcs.etrt.alt.supplier, tcmcs.etrt.direct.sched, tcmcs.etrt.pur.waiver, tcmcs.etrt.to.be.determine, tcmcs.etrt.pilot.material, tcmcs.etrt.mutually.def, tcmcs.etrt.not.appl |
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

# whwmd200

LN whwmd200 Warehouses table, Generated 2026-04-10T19:42:53Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_whwmd200` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `cwar` |
| **Column count** | 280 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `cwar` | `string` | `varchar` | `PK` | `not_null` | (required) Warehouse. Max length: 6 |
| `site` | `string` | `varchar` |  |  | Site. Max length: 9 |
| `sloc` | `integer` | `int` |  |  | Locations. Allowed values: 1, 2 |
| `sloc_kw` | `string` | `varchar` |  |  | Locations (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `usse` | `integer` | `int` |  |  | Use Site Settings. Allowed values: 1, 2 |
| `usse_kw` | `string` | `varchar` |  |  | Use Site Settings (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `itlo` | `integer` | `int` |  |  | Multi-Item Locations. Allowed values: 1, 2 |
| `itlo_kw` | `string` | `varchar` |  |  | Multi-Item Locations (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `lolo` | `integer` | `int` |  |  | Multi-Lot by Item Locations. Allowed values: 1, 2 |
| `lolo_kw` | `string` | `varchar` |  |  | Multi-Lot by Item Locations (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `free` | `float` | `float` |  |  | Minimum % Free for Inbound Advice |
| `ccal` | `string` | `varchar` |  |  | Calendar Code. Max length: 9 |
| `ract` | `string` | `varchar` |  |  | Availability Type. Max length: 6 |
| `arlo` | `string` | `varchar` |  |  | Receiving Location. Max length: 10 |
| `stlo` | `string` | `varchar` |  |  | Staging Location. Max length: 10 |
| `relo` | `string` | `varchar` |  |  | Quarantine Location. Max length: 10 |
| `palo` | `string` | `varchar` |  |  | Prepacking Advice Location. Max length: 10 |
| `ialt` | `integer` | `int` |  |  | Default Location Type for Inbound Advice. Allowed values: 1, 3, 4, 5, 6, 7, 20 |
| `ialt_kw` | `string` | `varchar` |  |  | Default Location Type for Inbound Advice (keyword). Allowed values: whwmd.loct.inspection, whwmd.loct.receiving, whwmd.loct.pick, whwmd.loct.bulk, whwmd.loct.staging, whwmd.loct.reject, whwmd.loct.not.appl |
| `uolo` | `integer` | `int` |  |  | Use Only Unoccupied Locations when Generating Inbound Advice. Allowed values: 1, 2 |
| `uolo_kw` | `string` | `varchar` |  |  | Use Only Unoccupied Locations when Generating Inbound Advice (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uudl` | `integer` | `int` |  |  | Use Only Unoccupied Dock Locations. Allowed values: 1, 2 |
| `uudl_kw` | `string` | `varchar` |  |  | Use Only Unoccupied Dock Locations (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ieod` | `integer` | `int` |  |  | Ignore Expected Decrease of Location Occupation. Allowed values: 1, 2 |
| `ieod_kw` | `string` | `varchar` |  |  | Ignore Expected Decrease of Location Occupation (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `frlo` | `integer` | `int` |  |  | Inbound Advice for Free (Unfixed) Locations. Allowed values: 1, 2 |
| `frlo_kw` | `string` | `varchar` |  |  | Inbound Advice for Free (Unfixed) Locations (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aoit` | `integer` | `int` |  |  | Allow Other Items on Fixed Multi-Item Locations. Allowed values: 1, 2 |
| `aoit_kw` | `string` | `varchar` |  |  | Allow Other Items on Fixed Multi-Item Locations (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `diws` | `integer` | `int` |  |  | Default Item Warehouse Status. Allowed values: 1, 2 |
| `diws_kw` | `string` | `varchar` |  |  | Default Item Warehouse Status (keyword). Allowed values: whwmd.iwhs.tentative, whwmd.iwhs.active |
| `iltm` | `float` | `float` |  |  | Inbound Lead Time |
| `iltu` | `integer` | `int` |  |  | Inbound Lead-time Unit. Allowed values: 5, 10 |
| `iltu_kw` | `string` | `varchar` |  |  | Inbound Lead-time Unit (keyword). Allowed values: tctope.hours, tctope.days |
| `oltm` | `float` | `float` |  |  | Outbound Lead Time |
| `oltu` | `integer` | `int` |  |  | Outbound Lead-time Unit. Allowed values: 5, 10 |
| `oltu_kw` | `string` | `varchar` |  |  | Outbound Lead-time Unit (keyword). Allowed values: tctope.hours, tctope.days |
| `rilt` | `float` | `float` |  |  | Rental Inbound Lead Time |
| `rilu` | `integer` | `int` |  |  | Rental Inbound Lead Time Unit. Allowed values: 5, 10 |
| `rilu_kw` | `string` | `varchar` |  |  | Rental Inbound Lead Time Unit (keyword). Allowed values: tctope.hours, tctope.days |
| `rolt` | `float` | `float` |  |  | Rental Outbound Lead Time |
| `rolu` | `integer` | `int` |  |  | Rental Outbound Lead Time Unit. Allowed values: 5, 10 |
| `rolu_kw` | `string` | `varchar` |  |  | Rental Outbound Lead Time Unit (keyword). Allowed values: tctope.hours, tctope.days |
| `reje` | `integer` | `int` |  |  | Quarantine Inventory. Allowed values: 1, 2 |
| `reje_kw` | `string` | `varchar` |  |  | Quarantine Inventory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `qwrh` | `string` | `varchar` |  |  | Quarantine Warehouse. Max length: 6 |
| `uhri` | `integer` | `int` |  |  | Use Handling Units in Quarantine Inventory. Allowed values: 1, 2 |
| `uhri_kw` | `string` | `varchar` |  |  | Use Handling Units in Quarantine Inventory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `zere` | `integer` | `int` |  |  | Zero Receipt Allowed. Allowed values: 1, 2 |
| `zere_kw` | `string` | `varchar` |  |  | Zero Receipt Allowed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cons` | `integer` | `int` |  |  | Confirm on Scan. Allowed values: 1, 2 |
| `cons_kw` | `string` | `varchar` |  |  | Confirm on Scan (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `olmc` | `string` | `varchar` |  |  | Shipment Handling Unit Mask. Max length: 9 |
| `sscc` | `integer` | `int` |  |  | Shipment Handling Unit Mask according to SSCC Standard. Allowed values: 1, 2 |
| `sscc_kw` | `string` | `varchar` |  |  | Shipment Handling Unit Mask according to SSCC Standard (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ilmc` | `string` | `varchar` |  |  | Internal Handling Unit Mask. Max length: 9 |
| `kmsk` | `string` | `varchar` |  |  | Kanban Signal ID Mask. Max length: 9 |
| `kdev` | `string` | `varchar` |  |  | Kanban Signal Label Device. Max length: 15 |
| `coka` | `integer` | `int` |  |  | Number of Copies of Kanban Signals |
| `rkbs` | `integer` | `int` |  |  | Reuse Kanban Signals. Allowed values: 1, 2 |
| `rkbs_kw` | `string` | `varchar` |  |  | Reuse Kanban Signals (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `karu` | `string` | `varchar` |  |  | Kanban Run Number. Max length: 12 |
| `gnsh` | `integer` | `int` |  |  | Generate Shipments For. Allowed values: 1, 2, 3, 4 |
| `gnsh_kw` | `string` | `varchar` |  |  | Generate Shipments For (keyword). Allowed values: whinh.gnsh.exact, whinh.gnsh.current, whinh.gnsh.upto, whinh.gnsh.interval |
| `misi` | `float` | `float` |  |  | Shipment Interval Lower Boundary |
| `misu` | `integer` | `int` |  |  | Shipment Interval Lower Boundary Unit. Allowed values: 5, 10 |
| `misu_kw` | `string` | `varchar` |  |  | Shipment Interval Lower Boundary Unit (keyword). Allowed values: tctope.hours, tctope.days |
| `masi` | `float` | `float` |  |  | Shipment Interval Upper Boundary |
| `masu` | `integer` | `int` |  |  | Shipment Interval Upper Boundary Unit. Allowed values: 5, 10 |
| `masu_kw` | `string` | `varchar` |  |  | Shipment Interval Upper Boundary Unit (keyword). Allowed values: tctope.hours, tctope.days |
| `orbo` | `integer` | `int` |  |  | Add Orders Based On. Allowed values: 1, 2 |
| `orbo_kw` | `string` | `varchar` |  |  | Add Orders Based On (keyword). Allowed values: whinh.orbo.pick, whinh.orbo.pddt |
| `smsh` | `integer` | `int` |  |  | Single Picking Mission per Shipment. Allowed values: 1, 2 |
| `smsh_kw` | `string` | `varchar` |  |  | Single Picking Mission per Shipment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `imdp` | `string` | `varchar` |  |  | Inventory Management Department. Max length: 6 |
| `wvgr` | `string` | `varchar` |  |  | Warehouse Valuation Group. Max length: 6 |
| `dycd` | `integer` | `int` |  |  | Dynamic Cross-docking. Allowed values: 1, 2 |
| `dycd_kw` | `string` | `varchar` |  |  | Dynamic Cross-docking (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdro` | `integer` | `int` |  |  | Generate Cross-dock Order when Releasing Order to Warehousing. Allowed values: 1, 2, 3, 4 |
| `cdro_kw` | `string` | `varchar` |  |  | Generate Cross-dock Order when Releasing Order to Warehousing (keyword). Allowed values: whinh.cgtr.never, whinh.cgtr.always, whinh.cgtr.time.phased, whinh.cgtr.time.phased.igr |
| `cdoa` | `integer` | `int` |  |  | Generate Cross-dock Order for Outbound Advice Shortage. Allowed values: 1, 2 |
| `cdoa_kw` | `string` | `varchar` |  |  | Generate Cross-dock Order for Outbound Advice Shortage (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdcr` | `integer` | `int` |  |  | Generate Cross-dock Order Lines when Confirming Receipt. Allowed values: 1, 2 |
| `cdcr_kw` | `string` | `varchar` |  |  | Generate Cross-dock Order Lines when Confirming Receipt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdap` | `integer` | `int` |  |  | Automatically Approve Cross-dock Order Lines. Allowed values: 1, 2 |
| `cdap_kw` | `string` | `varchar` |  |  | Automatically Approve Cross-dock Order Lines (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cdpd` | `string` | `varchar` |  |  | Priority Definition. Max length: 3 |
| `cdrd` | `string` | `varchar` |  |  | Cross-dock Restriction Definition. Max length: 3 |
| `cdlt` | `float` | `float` |  |  | Cross-dock Lead Time |
| `cdlu` | `integer` | `int` |  |  | Cross-dock Lead Time Unit. Allowed values: 5, 10 |
| `cdlu_kw` | `string` | `varchar` |  |  | Cross-dock Lead Time Unit (keyword). Allowed values: tctope.hours, tctope.days |
| `uhnd` | `integer` | `int` |  |  | Handling Units in Use. Allowed values: 1, 2 |
| `uhnd_kw` | `string` | `varchar` |  |  | Handling Units in Use (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uhir` | `integer` | `int` |  |  | Use Handling Unit in Receipt. Allowed values: 1, 2 |
| `uhir_kw` | `string` | `varchar` |  |  | Use Handling Unit in Receipt (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uhii` | `integer` | `int` |  |  | Use Handling Unit in Inbound Inspection. Allowed values: 1, 2 |
| `uhii_kw` | `string` | `varchar` |  |  | Use Handling Unit in Inbound Inspection (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uhin` | `integer` | `int` |  |  | Use Handling Unit in Inventory. Allowed values: 1, 2 |
| `uhin_kw` | `string` | `varchar` |  |  | Use Handling Unit in Inventory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uhoa` | `integer` | `int` |  |  | Use Handling Unit in Outbound Inspection. Allowed values: 1, 2 |
| `uhoa_kw` | `string` | `varchar` |  |  | Use Handling Unit in Outbound Inspection (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uhis` | `integer` | `int` |  |  | Use Handling Unit in Shipment. Allowed values: 1, 2 |
| `uhis_kw` | `string` | `varchar` |  |  | Use Handling Unit in Shipment (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `gehu` | `integer` | `int` |  |  | Generate Handling Unit Automatically from ASNs. Allowed values: 1, 2, 3, 4 |
| `gehu_kw` | `string` | `varchar` |  |  | Generate Handling Unit Automatically from ASNs (keyword). Allowed values: whinh.gehu.always, whinh.gehu.exhu.received, whinh.gehu.no, whinh.gehu.register |
| `gehr` | `integer` | `int` |  |  | Generate Handling Unit Automatically during Receipt. Allowed values: 1, 2, 3 |
| `gehr_kw` | `string` | `varchar` |  |  | Generate Handling Unit Automatically during Receipt (keyword). Allowed values: whinh.gehu.aut.always, whinh.gehu.aut.register, whinh.gehu.aut.never |
| `gehp` | `integer` | `int` |  |  | Generate Handling Unit Automatically during Picking. Allowed values: 1, 2 |
| `gehp_kw` | `string` | `varchar` |  |  | Generate Handling Unit Automatically during Picking (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `geha` | `integer` | `int` |  |  | Generate Handling Unit Automatically during Adjustment. Allowed values: 1, 2, 3 |
| `geha_kw` | `string` | `varchar` |  |  | Generate Handling Unit Automatically during Adjustment (keyword). Allowed values: whinh.gehu.aut.always, whinh.gehu.aut.register, whinh.gehu.aut.never |
| `gehc` | `integer` | `int` |  |  | Generate Handling Unit Automatically during Cycle Counting. Allowed values: 1, 2, 3 |
| `gehc_kw` | `string` | `varchar` |  |  | Generate Handling Unit Automatically during Cycle Counting (keyword). Allowed values: whinh.gehu.aut.always, whinh.gehu.aut.register, whinh.gehu.aut.never |
| `ghps` | `integer` | `int` |  |  | Generate Handling Unit Automatically during Projected Shipments. Allowed values: 1, 2 |
| `ghps_kw` | `string` | `varchar` |  |  | Generate Handling Unit Automatically during Projected Shipments (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prlu` | `integer` | `int` |  |  | Print Label during ASN Creation. Allowed values: 1, 2, 10 |
| `prlu_kw` | `string` | `varchar` |  |  | Print Label during ASN Creation (keyword). Allowed values: whinh.prla.yes, whinh.prla.no, whinh.prla.hu.only |
| `prlr` | `integer` | `int` |  |  | Print Label during Receipt. Allowed values: 1, 2, 10 |
| `prlr_kw` | `string` | `varchar` |  |  | Print Label during Receipt (keyword). Allowed values: whinh.prla.yes, whinh.prla.no, whinh.prla.hu.only |
| `prlp` | `integer` | `int` |  |  | Print Label during Picking. Allowed values: 1, 2, 10 |
| `prlp_kw` | `string` | `varchar` |  |  | Print Label during Picking (keyword). Allowed values: whinh.prla.yes, whinh.prla.no, whinh.prla.hu.only |
| `prla` | `integer` | `int` |  |  | Print Handling Unit Label during Adjustment. Allowed values: 1, 2, 10 |
| `prla_kw` | `string` | `varchar` |  |  | Print Handling Unit Label during Adjustment (keyword). Allowed values: whinh.prla.yes, whinh.prla.no, whinh.prla.hu.only |
| `prlc` | `integer` | `int` |  |  | Print Handling Unit Label during Cycle Counting. Allowed values: 1, 2, 10 |
| `prlc_kw` | `string` | `varchar` |  |  | Print Handling Unit Label during Cycle Counting (keyword). Allowed values: whinh.prla.yes, whinh.prla.no, whinh.prla.hu.only |
| `copu` | `integer` | `int` |  |  | Number of Copies of Labels during ASN Creation |
| `copr` | `integer` | `int` |  |  | Number of Copies of Labels during Receipt |
| `copp` | `integer` | `int` |  |  | Number of Copies of Labels during Picking |
| `copa` | `integer` | `int` |  |  | Number of Copies of Labels for Handling Units during Adjustment |
| `copc` | `integer` | `int` |  |  | Number of Copies of Labels for Handling Units during Cycle Counting |
| `lpas` | `integer` | `int` |  |  | Label printed by during creation ASN. Allowed values: 10, 20 |
| `lpas_kw` | `string` | `varchar` |  |  | Label printed by during creation ASN (keyword). Allowed values: whwmd.lbpb.internal, whwmd.lbpb.external |
| `lpre` | `integer` | `int` |  |  | Label printed by during confirm receipts. Allowed values: 10, 20 |
| `lpre_kw` | `string` | `varchar` |  |  | Label printed by during confirm receipts (keyword). Allowed values: whwmd.lbpb.internal, whwmd.lbpb.external |
| `lppi` | `integer` | `int` |  |  | Label printed by during confirm picking. Allowed values: 10, 20 |
| `lppi_kw` | `string` | `varchar` |  |  | Label printed by during confirm picking (keyword). Allowed values: whwmd.lbpb.internal, whwmd.lbpb.external |
| `lpad` | `integer` | `int` |  |  | Label printed by during process adjustment orders. Allowed values: 10, 20 |
| `lpad_kw` | `string` | `varchar` |  |  | Label printed by during process adjustment orders (keyword). Allowed values: whwmd.lbpb.internal, whwmd.lbpb.external |
| `lpcc` | `integer` | `int` |  |  | Label printed by during process cycle counting orders. Allowed values: 10, 20 |
| `lpcc_kw` | `string` | `varchar` |  |  | Label printed by during process cycle counting orders (keyword). Allowed values: whwmd.lbpb.internal, whwmd.lbpb.external |
| `prmu` | `integer` | `int` |  |  | Printing Method for Labels during ASN Creation. Allowed values: 10, 20, 30 |
| `prmu_kw` | `string` | `varchar` |  |  | Printing Method for Labels during ASN Creation (keyword). Allowed values: whinh.prmt.by.line, whinh.prmt.by.unit, whinh.prmt.not.appl |
| `prmr` | `integer` | `int` |  |  | Printing Method for Labels during Receipts. Allowed values: 10, 20, 30 |
| `prmr_kw` | `string` | `varchar` |  |  | Printing Method for Labels during Receipts (keyword). Allowed values: whinh.prmt.by.line, whinh.prmt.by.unit, whinh.prmt.not.appl |
| `prmp` | `integer` | `int` |  |  | Printing Method for Labels during Picking. Allowed values: 10, 20, 30 |
| `prmp_kw` | `string` | `varchar` |  |  | Printing Method for Labels during Picking (keyword). Allowed values: whinh.prmt.by.line, whinh.prmt.by.unit, whinh.prmt.not.appl |
| `labr` | `string` | `varchar` |  |  | Default Label Layout in Inbound. Max length: 3 |
| `labi` | `string` | `varchar` |  |  | Obsolete. Max length: 3 |
| `laba` | `string` | `varchar` |  |  | Default Label Layout for Handling Unit in Adjustment. Max length: 3 |
| `labc` | `string` | `varchar` |  |  | Default Label Layout for Handling Unit in Cycle Counting. Max length: 3 |
| `labo` | `string` | `varchar` |  |  | Default Label Layout in Outbound. Max length: 3 |
| `flhu` | `integer` | `int` |  |  | Automatic Labeling of Multi-Item Handling Unit. Allowed values: 1, 2 |
| `flhu_kw` | `string` | `varchar` |  |  | Automatic Labeling of Multi-Item Handling Unit (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `aipd` | `integer` | `int` |  |  | Advise Alternative Package Definition Allowed. Allowed values: 1, 2 |
| `aipd_kw` | `string` | `varchar` |  |  | Advise Alternative Package Definition Allowed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `shhu` | `integer` | `int` |  |  | Generate Handling Unit for Shipment Header during Picking. Allowed values: 1, 2 |
| `shhu_kw` | `string` | `varchar` |  |  | Generate Handling Unit for Shipment Header during Picking (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mrhu` | `integer` | `int` |  |  | Consolidate Handling Units in one Shipment Line during Picking. Allowed values: 1, 2 |
| `mrhu_kw` | `string` | `varchar` |  |  | Consolidate Handling Units in one Shipment Line during Picking (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `flup` | `integer` | `int` |  |  | Fill Up Staged Handling Units with Newly Picked Items. Allowed values: 1, 2 |
| `flup_kw` | `string` | `varchar` |  |  | Fill Up Staged Handling Units with Newly Picked Items (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `trdc` | `integer` | `int` |  |  | Transport Document. Allowed values: 1, 2, 5, 10 |
| `trdc_kw` | `string` | `varchar` |  |  | Transport Document (keyword). Allowed values: whwmd.tran.doc.del.note, whwmd.tran.doc.invoice, whwmd.tran.doc.order.procedure, whwmd.tran.doc.not.appl |
| `spps` | `integer` | `int` |  |  | Suppress Printing Packing Slip. Allowed values: 1, 2, 3 |
| `spps_kw` | `string` | `varchar` |  |  | Suppress Printing Packing Slip (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `usmd` | `integer` | `int` |  |  | Update Shipping Material Account during. Allowed values: 10, 20, 100 |
| `usmd_kw` | `string` | `varchar` |  |  | Update Shipping Material Account during (keyword). Allowed values: whwmd.usmd.ship.to.vmi.wh, whwmd.usmd.consume.vmi.wh, whwmd.usmd.not.appl |
| `rsdn` | `integer` | `int` |  |  | Reset Delivery Note Number. Allowed values: 1, 2 |
| `rsdn_kw` | `string` | `varchar` |  |  | Reset Delivery Note Number (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rtcf` | `integer` | `int` |  |  | Return Certificate. Allowed values: 1, 2 |
| `rtcf_kw` | `string` | `varchar` |  |  | Return Certificate (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rsrc` | `integer` | `int` |  |  | Reset Return Certificate. Allowed values: 1, 2 |
| `rsrc_kw` | `string` | `varchar` |  |  | Reset Return Certificate (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dmsp` | `integer` | `int` |  |  | DMS Supplied. Allowed values: 1, 2 |
| `dmsp_kw` | `string` | `varchar` |  |  | DMS Supplied (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dmsr` | `integer` | `int` |  |  | DMS upon Receipt. Allowed values: 10, 20, 30, 40 |
| `dmsr_kw` | `string` | `varchar` |  |  | DMS upon Receipt (keyword). Allowed values: whwmd.dmsr.no, whwmd.dmsr.automatic, whwmd.dmsr.interactive, whwmd.dmsr.manual |
| `dmss` | `integer` | `int` |  |  | DMS upon JSC Receipt. Allowed values: 10, 20, 30, 40 |
| `dmss_kw` | `string` | `varchar` |  |  | DMS upon JSC Receipt (keyword). Allowed values: whwmd.dmsr.no, whwmd.dmsr.automatic, whwmd.dmsr.interactive, whwmd.dmsr.manual |
| `dmsi` | `integer` | `int` |  |  | DMS on Inventory. Allowed values: 10, 20, 30 |
| `dmsi_kw` | `string` | `varchar` |  |  | DMS on Inventory (keyword). Allowed values: whwmd.dmsi.no, whwmd.dmsi.outbound, whwmd.dmsi.rec.and.outb |
| `dmdt` | `integer` | `int` |  |  | Demand Type for DMS upon Receipt. Allowed values: 10, 20, 30, 40, 100 |
| `dmdt_kw` | `string` | `varchar` |  |  | Demand Type for DMS upon Receipt (keyword). Allowed values: whwmd.dmdt.wh.order, whwmd.dmdt.plan.inv.trans, whwmd.dmdt.planned.order, whwmd.dmdt.forecast, whwmd.dmdt.not.appl |
| `ddwr` | `integer` | `int` |  |  | DMS upon Receipt for Warehousing Orders from Status. Allowed values: 2, 3, 5, 10, 15, 20, 22, 23, 25, 30 |
| `ddwr_kw` | `string` | `varchar` |  |  | DMS upon Receipt for Warehousing Orders from Status (keyword). Allowed values: whinh.lstb.planned, whinh.lstb.tag.linked, whinh.lstb.open, whinh.lstb.adviced, whinh.lstb.released, whinh.lstb.picked, whinh.lstb.to.be.inspected, whinh.lstb.inspected, whinh.lstb.staged, whinh.lstb.shipped |
| `ddti` | `integer` | `int` |  |  | Demand Type for DMS on Inventory. Allowed values: 10, 20, 30, 40, 100 |
| `ddti_kw` | `string` | `varchar` |  |  | Demand Type for DMS on Inventory (keyword). Allowed values: whwmd.dmdt.wh.order, whwmd.dmdt.plan.inv.trans, whwmd.dmdt.planned.order, whwmd.dmdt.forecast, whwmd.dmdt.not.appl |
| `ddwi` | `integer` | `int` |  |  | DMS on Inventory for Warehousing Orders from Status. Allowed values: 2, 3, 5, 10, 15, 20, 22, 23, 25, 30 |
| `ddwi_kw` | `string` | `varchar` |  |  | DMS on Inventory for Warehousing Orders from Status (keyword). Allowed values: whinh.lstb.planned, whinh.lstb.tag.linked, whinh.lstb.open, whinh.lstb.adviced, whinh.lstb.released, whinh.lstb.picked, whinh.lstb.to.be.inspected, whinh.lstb.inspected, whinh.lstb.staged, whinh.lstb.shipped |
| `dmph` | `float` | `float` |  |  | Planning Horizon for DMS upon Receipt |
| `dphi` | `float` | `float` |  |  | Planning Horizon for DMS on Inventory |
| `dprl` | `integer` | `int` |  |  | Print Direct Material Supply Label. Allowed values: 1, 2 |
| `dprl_kw` | `string` | `varchar` |  |  | Print Direct Material Supply Label (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dmsl` | `string` | `varchar` |  |  | Direct Material Supply Label Layout. Max length: 3 |
| `dptr` | `integer` | `int` |  |  | Print Direct Material Supply Transfer Report. Allowed values: 1, 2 |
| `dptr_kw` | `string` | `varchar` |  |  | Print Direct Material Supply Transfer Report (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mitt` | `float` | `float` |  |  | Minimum Time Fence Tolerance |
| `mitu` | `integer` | `int` |  |  | Minimum Time Fence Tolerance Unit. Allowed values: 5, 10 |
| `mitu_kw` | `string` | `varchar` |  |  | Minimum Time Fence Tolerance Unit (keyword). Allowed values: tctope.hours, tctope.days |
| `matt` | `float` | `float` |  |  | Maximum Time Fence Tolerance |
| `matu` | `integer` | `int` |  |  | Maximum Time Fence Tolerance Unit. Allowed values: 5, 10 |
| `matu_kw` | `string` | `varchar` |  |  | Maximum Time Fence Tolerance Unit (keyword). Allowed values: tctope.hours, tctope.days |
| `qfmi` | `float` | `float` |  |  | Force Cross-dock Minimum Quantity |
| `qfma` | `float` | `float` |  |  | Force Cross-dock Maximum Quantity |
| `prva` | `integer` | `int` |  |  | Process Inventory Variances Automatically. Allowed values: 1, 2 |
| `prva_kw` | `string` | `varchar` |  |  | Process Inventory Variances Automatically (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `fire` | `integer` | `int` |  |  | Confirm Receipt as Final. Allowed values: 1, 2 |
| `fire_kw` | `string` | `varchar` |  |  | Confirm Receipt as Final (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `mipa` | `integer` | `int` |  |  | Manual Inbound Process Allowed. Allowed values: 1, 2, 3 |
| `mipa_kw` | `string` | `varchar` |  |  | Manual Inbound Process Allowed (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `mopa` | `integer` | `int` |  |  | Manual Outbound Process Allowed. Allowed values: 1, 2, 3 |
| `mopa_kw` | `string` | `varchar` |  |  | Manual Outbound Process Allowed (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `mada` | `integer` | `int` |  |  | Manual Adjustments Allowed. Allowed values: 1, 2, 3 |
| `mada_kw` | `string` | `varchar` |  |  | Manual Adjustments Allowed (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `vpad` | `integer` | `int` |  |  | Obsolete. Allowed values: 10, 20 |
| `vpad_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: whinh.vpad.ftp, whinh.vpad.inv.value |
| `mcca` | `integer` | `int` |  |  | Manual Cycle Counting Allowed. Allowed values: 1, 2, 3 |
| `mcca_kw` | `string` | `varchar` |  |  | Manual Cycle Counting Allowed (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `vpcc` | `integer` | `int` |  |  | Obsolete. Allowed values: 10, 20 |
| `vpcc_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: whinh.vpad.ftp, whinh.vpad.inv.value |
| `bcyc` | `integer` | `int` |  |  | Block During Warehouse Cycle Counting. Allowed values: 1, 2 |
| `bcyc_kw` | `string` | `varchar` |  |  | Block During Warehouse Cycle Counting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `uwtr` | `integer` | `int` |  |  | Usage at Warehouse Transfer. Allowed values: 10, 20, 30, 40, 70, 90 |
| `uwtr_kw` | `string` | `varchar` |  |  | Usage at Warehouse Transfer (keyword). Allowed values: tcuwtr.always, tcuwtr.cluster, tcuwtr.enterprise.unit, tcuwtr.fin.company, tcuwtr.no, tcuwtr.not.appl |
| `binb` | `integer` | `int` |  |  | Blocked for Inbound. Allowed values: 10, 20, 30 |
| `binb_kw` | `string` | `varchar` |  |  | Blocked for Inbound (keyword). Allowed values: whwmd.byni.yes, whwmd.byni.interactive, whwmd.byni.no |
| `bout` | `integer` | `int` |  |  | Blocked for Outbound. Allowed values: 10, 20, 30 |
| `bout_kw` | `string` | `varchar` |  |  | Blocked for Outbound (keyword). Allowed values: whwmd.byni.yes, whwmd.byni.interactive, whwmd.byni.no |
| `bloc` | `string` | `varchar` |  |  | Reason for Blocking. Max length: 6 |
| `ufpl` | `integer` | `int` |  |  | Use Fulfillment Plan. Allowed values: 1, 2 |
| `ufpl_kw` | `string` | `varchar` |  |  | Use Fulfillment Plan (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sups` | `integer` | `int` |  |  | Default Supply System. Allowed values: 1, 2, 3, 4, 5, 6 |
| `sups_kw` | `string` | `varchar` |  |  | Default Supply System (keyword). Allowed values: tcsups.tpop, tcsups.kanban, tcsups.batch, tcsups.sils, tcsups.single, tcsups.none |
| `sfwh` | `integer` | `int` |  |  | Supply from Warehouse. Allowed values: 1, 2 |
| `sfwh_kw` | `string` | `varchar` |  |  | Supply from Warehouse (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `scom` | `integer` | `int` |  |  | Supply Company |
| `supw` | `string` | `varchar` |  |  | Supply Warehouse. Max length: 6 |
| `uidt` | `integer` | `int` |  |  | Use Item Ordering Data. Allowed values: 1, 2 |
| `uidt_kw` | `string` | `varchar` |  |  | Use Item Ordering Data (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `shtw` | `string` | `varchar` |  |  | Shipment through Warehouse. Max length: 6 |
| `qmoo` | `integer` | `int` |  |  | QM Overrules Warehouse Inbound Order Type. Allowed values: 1, 2 |
| `qmoo_kw` | `string` | `varchar` |  |  | QM Overrules Warehouse Inbound Order Type (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `rfsa` | `integer` | `int` |  |  | RFID during Shipping Applicable. Allowed values: 1, 2 |
| `rfsa_kw` | `string` | `varchar` |  |  | RFID during Shipping Applicable (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cwar_arlo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd300 Locations |
| `cwar_stlo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd300 Locations |
| `cwar_relo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd300 Locations |
| `cwar_palo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd300 Locations |
| `cwar_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `site_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm050 Sites |
| `ccal_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp010 Calendar Codes |
| `ract_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp001 Availability Types |
| `qwrh_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `olmc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd402 Masks |
| `ilmc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd402 Masks |
| `kmsk_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd402 Masks |
| `karu_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh400 Runs |
| `imdp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd020 Inventory Management Departments |
| `wvgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whina102 Warehouse Valuation Groups |
| `cdpd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh620 Priority Definitions |
| `cdrd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whinh650 Cross-dock Restriction Definitions |
| `labr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd520 Label Layouts |
| `labi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd520 Label Layouts |
| `laba_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd520 Label Layouts |
| `labc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd520 Label Layouts |
| `labo_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd520 Label Layouts |
| `dmsl_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table whwmd520 Label Layouts |
| `bloc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `supw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
| `shtw_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs003 Warehouses |
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

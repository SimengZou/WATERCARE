# tsmdm000

LN tsmdm000 General Service Parameters table, Generated 2026-04-10T19:42:35Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tsmdm000` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `indt` |
| **Column count** | 174 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `indt` | `timestamp` | `timestamp_ntz` | `PK` | `not_null` | (required) Introduction Date |
| `dsca__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 50 |
| `trmd` | `integer` | `int` |  |  | Traveling Cost Method. Allowed values: 2, 5, 10, 15, 18, 20 |
| `trmd_kw` | `string` | `varchar` |  |  | Traveling Cost Method (keyword). Allowed values: tsmdm.trmd.serial, tsmdm.trmd.installation, tsmdm.trmd.area, tsmdm.trmd.zone, tsmdm.trmd.so.travel, tsmdm.trmd.none |
| `usar` | `integer` | `int` |  |  | Use Service Areas. Allowed values: 1, 2 |
| `usar_kw` | `string` | `varchar` |  |  | Use Service Areas (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `undi` | `string` | `varchar` |  |  | Distance Unit. Max length: 3 |
| `untm` | `string` | `varchar` |  |  | Base Time Unit Hour. Max length: 3 |
| `unlr` | `string` | `varchar` |  |  | Labor Rate Time Unit. Max length: 3 |
| `untd` | `string` | `varchar` |  |  | Time Duration Unit. Max length: 3 |
| `uday` | `string` | `varchar` |  |  | Unit for Day. Max length: 3 |
| `uwks` | `string` | `varchar` |  |  | Unit for Week. Max length: 3 |
| `umon` | `string` | `varchar` |  |  | Unit for Month. Max length: 3 |
| `uyrs` | `string` | `varchar` |  |  | Unit for Year. Max length: 3 |
| `trac_1` | `float` | `float` |  |  | Standard Cost for Traveling Time |
| `trac_2` | `float` | `float` |  |  | Standard Cost for Traveling Time |
| `trac_3` | `float` | `float` |  |  | Standard Cost for Traveling Time |
| `tras` | `float` | `float` |  |  | Standard Sales Price for Traveling Time |
| `cpdi_1` | `float` | `float` |  |  | Standard Cost for Traveling Distance |
| `cpdi_2` | `float` | `float` |  |  | Standard Cost for Traveling Distance |
| `cpdi_3` | `float` | `float` |  |  | Standard Cost for Traveling Distance |
| `spdi` | `float` | `float` |  |  | Standard Sales Price for Traveling Distance |
| `tvcc` | `string` | `varchar` |  |  | Traveling Cost Currency. Max length: 3 |
| `tvce` | `integer` | `int` |  |  | Update Traveling Cost Currency Executed. Allowed values: 1, 2 |
| `tvce_kw` | `string` | `varchar` |  |  | Update Traveling Cost Currency Executed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ccur` | `string` | `varchar` |  |  | Travel Sales Price Currency. Max length: 3 |
| `erac` | `integer` | `int` |  |  | Engineering Revisions Active. Allowed values: 1, 2 |
| `erac_kw` | `string` | `varchar` |  |  | Engineering Revisions Active (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `clyn` | `integer` | `int` |  |  | Apply Search Engine for Installation Groups. Allowed values: 1, 2 |
| `clyn_kw` | `string` | `varchar` |  |  | Apply Search Engine for Installation Groups (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `siyn` | `integer` | `int` |  |  | Apply Search Engine for Serialized Items. Allowed values: 1, 2 |
| `siyn_kw` | `string` | `varchar` |  |  | Apply Search Engine for Serialized Items (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `atws_1` | `integer` | `int` |  |  | Attribute Search Argument Location Address. Allowed values: 5, 10, 15, 20, 25, 30, 35, 37, 40, 45, 50, 55, 99 |
| `atws_kw_1` | `string` | `varchar` |  |  | Attribute Search Argument Location Address (keyword). Allowed values: tsmdm.atrb.name, tsmdm.atrb.name2, tsmdm.atrb.houseno, tsmdm.atrb.address, tsmdm.atrb.address2, tsmdm.atrb.town, tsmdm.atrb.town2, tsmdm.atrb.towndsca, tsmdm.atrb.zipcode, tsmdm.atrb.ccty, tsmdm.atrb.country, tsmdm.atrb.telp, tsmdm.atrb.not.applicable |
| `atws_2` | `integer` | `int` |  |  | Attribute Search Argument Location Address. Allowed values: 5, 10, 15, 20, 25, 30, 35, 37, 40, 45, 50, 55, 99 |
| `atws_kw_2` | `string` | `varchar` |  |  | Attribute Search Argument Location Address (keyword). Allowed values: tsmdm.atrb.name, tsmdm.atrb.name2, tsmdm.atrb.houseno, tsmdm.atrb.address, tsmdm.atrb.address2, tsmdm.atrb.town, tsmdm.atrb.town2, tsmdm.atrb.towndsca, tsmdm.atrb.zipcode, tsmdm.atrb.ccty, tsmdm.atrb.country, tsmdm.atrb.telp, tsmdm.atrb.not.applicable |
| `atws_3` | `integer` | `int` |  |  | Attribute Search Argument Location Address. Allowed values: 5, 10, 15, 20, 25, 30, 35, 37, 40, 45, 50, 55, 99 |
| `atws_kw_3` | `string` | `varchar` |  |  | Attribute Search Argument Location Address (keyword). Allowed values: tsmdm.atrb.name, tsmdm.atrb.name2, tsmdm.atrb.houseno, tsmdm.atrb.address, tsmdm.atrb.address2, tsmdm.atrb.town, tsmdm.atrb.town2, tsmdm.atrb.towndsca, tsmdm.atrb.zipcode, tsmdm.atrb.ccty, tsmdm.atrb.country, tsmdm.atrb.telp, tsmdm.atrb.not.applicable |
| `atws_4` | `integer` | `int` |  |  | Attribute Search Argument Location Address. Allowed values: 5, 10, 15, 20, 25, 30, 35, 37, 40, 45, 50, 55, 99 |
| `atws_kw_4` | `string` | `varchar` |  |  | Attribute Search Argument Location Address (keyword). Allowed values: tsmdm.atrb.name, tsmdm.atrb.name2, tsmdm.atrb.houseno, tsmdm.atrb.address, tsmdm.atrb.address2, tsmdm.atrb.town, tsmdm.atrb.town2, tsmdm.atrb.towndsca, tsmdm.atrb.zipcode, tsmdm.atrb.ccty, tsmdm.atrb.country, tsmdm.atrb.telp, tsmdm.atrb.not.applicable |
| `atws_5` | `integer` | `int` |  |  | Attribute Search Argument Location Address. Allowed values: 5, 10, 15, 20, 25, 30, 35, 37, 40, 45, 50, 55, 99 |
| `atws_kw_5` | `string` | `varchar` |  |  | Attribute Search Argument Location Address (keyword). Allowed values: tsmdm.atrb.name, tsmdm.atrb.name2, tsmdm.atrb.houseno, tsmdm.atrb.address, tsmdm.atrb.address2, tsmdm.atrb.town, tsmdm.atrb.town2, tsmdm.atrb.towndsca, tsmdm.atrb.zipcode, tsmdm.atrb.ccty, tsmdm.atrb.country, tsmdm.atrb.telp, tsmdm.atrb.not.applicable |
| `atws_6` | `integer` | `int` |  |  | Attribute Search Argument Location Address. Allowed values: 5, 10, 15, 20, 25, 30, 35, 37, 40, 45, 50, 55, 99 |
| `atws_kw_6` | `string` | `varchar` |  |  | Attribute Search Argument Location Address (keyword). Allowed values: tsmdm.atrb.name, tsmdm.atrb.name2, tsmdm.atrb.houseno, tsmdm.atrb.address, tsmdm.atrb.address2, tsmdm.atrb.town, tsmdm.atrb.town2, tsmdm.atrb.towndsca, tsmdm.atrb.zipcode, tsmdm.atrb.ccty, tsmdm.atrb.country, tsmdm.atrb.telp, tsmdm.atrb.not.applicable |
| `atws_7` | `integer` | `int` |  |  | Attribute Search Argument Location Address. Allowed values: 5, 10, 15, 20, 25, 30, 35, 37, 40, 45, 50, 55, 99 |
| `atws_kw_7` | `string` | `varchar` |  |  | Attribute Search Argument Location Address (keyword). Allowed values: tsmdm.atrb.name, tsmdm.atrb.name2, tsmdm.atrb.houseno, tsmdm.atrb.address, tsmdm.atrb.address2, tsmdm.atrb.town, tsmdm.atrb.town2, tsmdm.atrb.towndsca, tsmdm.atrb.zipcode, tsmdm.atrb.ccty, tsmdm.atrb.country, tsmdm.atrb.telp, tsmdm.atrb.not.applicable |
| `atws_8` | `integer` | `int` |  |  | Attribute Search Argument Location Address. Allowed values: 5, 10, 15, 20, 25, 30, 35, 37, 40, 45, 50, 55, 99 |
| `atws_kw_8` | `string` | `varchar` |  |  | Attribute Search Argument Location Address (keyword). Allowed values: tsmdm.atrb.name, tsmdm.atrb.name2, tsmdm.atrb.houseno, tsmdm.atrb.address, tsmdm.atrb.address2, tsmdm.atrb.town, tsmdm.atrb.town2, tsmdm.atrb.towndsca, tsmdm.atrb.zipcode, tsmdm.atrb.ccty, tsmdm.atrb.country, tsmdm.atrb.telp, tsmdm.atrb.not.applicable |
| `atws_9` | `integer` | `int` |  |  | Attribute Search Argument Location Address. Allowed values: 5, 10, 15, 20, 25, 30, 35, 37, 40, 45, 50, 55, 99 |
| `atws_kw_9` | `string` | `varchar` |  |  | Attribute Search Argument Location Address (keyword). Allowed values: tsmdm.atrb.name, tsmdm.atrb.name2, tsmdm.atrb.houseno, tsmdm.atrb.address, tsmdm.atrb.address2, tsmdm.atrb.town, tsmdm.atrb.town2, tsmdm.atrb.towndsca, tsmdm.atrb.zipcode, tsmdm.atrb.ccty, tsmdm.atrb.country, tsmdm.atrb.telp, tsmdm.atrb.not.applicable |
| `atws_10` | `integer` | `int` |  |  | Attribute Search Argument Location Address. Allowed values: 5, 10, 15, 20, 25, 30, 35, 37, 40, 45, 50, 55, 99 |
| `atws_kw_10` | `string` | `varchar` |  |  | Attribute Search Argument Location Address (keyword). Allowed values: tsmdm.atrb.name, tsmdm.atrb.name2, tsmdm.atrb.houseno, tsmdm.atrb.address, tsmdm.atrb.address2, tsmdm.atrb.town, tsmdm.atrb.town2, tsmdm.atrb.towndsca, tsmdm.atrb.zipcode, tsmdm.atrb.ccty, tsmdm.atrb.country, tsmdm.atrb.telp, tsmdm.atrb.not.applicable |
| `spsm` | `integer` | `int` |  |  | Sales Prices Search Method. Allowed values: 1, 2, 3, 4 |
| `spsm_kw` | `string` | `varchar` |  |  | Sales Prices Search Method (keyword). Allowed values: tsmdm.spsm.serv.books, tsmdm.spsm.serv.sls.books, tsmdm.spsm.serv.items, tsmdm.spsm.sls.serv.books |
| `feyn` | `integer` | `int` |  |  | Functional Elements Active. Allowed values: 0, 1, 2 |
| `feyn_kw` | `string` | `varchar` |  |  | Functional Elements Active (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `flds` | `integer` | `int` |  |  | Field Service. Allowed values: 1, 2 |
| `flds_kw` | `string` | `varchar` |  |  | Field Service (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `shpm` | `integer` | `int` |  |  | Depot Repair. Allowed values: 1, 2 |
| `shpm_kw` | `string` | `varchar` |  |  | Depot Repair (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `skll` | `integer` | `int` |  |  | Skills Implemented. Allowed values: 1, 2 |
| `skll_kw` | `string` | `varchar` |  |  | Skills Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `meyn` | `integer` | `int` |  |  | Use Inspections. Allowed values: 1, 2 |
| `meyn_kw` | `string` | `varchar` |  |  | Use Inspections (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `icrr` | `integer` | `int` |  |  | Use Item Counter Readings. Allowed values: 0, 1, 2 |
| `icrr_kw` | `string` | `varchar` |  |  | Use Item Counter Readings (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `ccli` | `integer` | `int` |  |  | Customer Claims. Allowed values: 1, 2 |
| `ccli_kw` | `string` | `varchar` |  |  | Customer Claims (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `scli` | `integer` | `int` |  |  | Supplier Claims. Allowed values: 1, 2 |
| `scli_kw` | `string` | `varchar` |  |  | Supplier Claims (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ract` | `string` | `varchar` |  |  | Availability Type. Max length: 6 |
| `dfpb` | `integer` | `int` |  |  | Discounts from Price Books. Allowed values: 0, 1, 2 |
| `dfpb_kw` | `string` | `varchar` |  |  | Discounts from Price Books (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `dipy` | `integer` | `int` |  |  | Discount Policy. Allowed values: 1, 2 |
| `dipy_kw` | `string` | `varchar` |  |  | Discount Policy (keyword). Allowed values: tsmdm.dipy.bfcv, tsmdm.dipy.afcv |
| `utpd` | `integer` | `int` |  |  | Use Cumulative and Total Prices and Discounts. Allowed values: 1, 2 |
| `utpd_kw` | `string` | `varchar` |  |  | Use Cumulative and Total Prices and Discounts (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ubpp` | `integer` | `int` |  |  | Use Default Business Partner Prices and Discounts. Allowed values: 1, 2 |
| `ubpp_kw` | `string` | `varchar` |  |  | Use Default Business Partner Prices and Discounts (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dist` | `integer` | `int` |  |  | Default Invoice Line Status. Allowed values: 5, 10 |
| `dist_kw` | `string` | `varchar` |  |  | Default Invoice Line Status (keyword). Allowed values: tsmdm.inst.on.hold, tsmdm.inst.confirmed |
| `alrm` | `integer` | `int` |  |  | Demand Pegging. Allowed values: 1, 2 |
| `alrm_kw` | `string` | `varchar` |  |  | Demand Pegging (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `isdp` | `integer` | `int` |  |  | Use Item Settings for Demand Pegging. Allowed values: 1, 2 |
| `isdp_kw` | `string` | `varchar` |  |  | Use Item Settings for Demand Pegging (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `prnt` | `integer` | `int` |  |  | Default Value for Print on Invoice. Allowed values: 1, 2, 3, 4 |
| `prnt_kw` | `string` | `varchar` |  |  | Default Value for Print on Invoice (keyword). Allowed values: tsmdm.prnt.yes, tsmdm.prnt.no, tsmdm.prnt.not.app, tsmdm.prnt.no.invoice |
| `pvap` | `float` | `float` |  |  | Percentage of Valuation Price |
| `uspg` | `integer` | `int` |  |  | Paging Active. Allowed values: 1, 2 |
| `uspg_kw` | `string` | `varchar` |  |  | Paging Active (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `udgn` | `integer` | `int` |  |  | Use Diagnostics. Allowed values: 1, 2 |
| `udgn_kw` | `string` | `varchar` |  |  | Use Diagnostics (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ncri` | `integer` | `int` |  |  | Non-Conformance Reports Implemented. Allowed values: 1, 2 |
| `ncri_kw` | `string` | `varchar` |  |  | Non-Conformance Reports Implemented (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `scin` | `integer` | `int` |  |  | Scope of Inventory Check. Allowed values: 5, 10 |
| `scin_kw` | `string` | `varchar` |  |  | Scope of Inventory Check (keyword). Allowed values: tsmdm.scin.curr.warehouse, tsmdm.scin.wrhs.in.cluster |
| `icpr_1` | `integer` | `int` |  |  | Order Type Priority Material Availability Check. Allowed values: 5, 10, 15, 20, 25, 99 |
| `icpr_kw_1` | `string` | `varchar` |  |  | Order Type Priority Material Availability Check (keyword). Allowed values: tsmdm.maco.soc, tsmdm.maco.wcs, tsmdm.maco.msc, tsmdm.maco.spc, tsmdm.maco.epp, tsmdm.maco.not.applicable |
| `icpr_2` | `integer` | `int` |  |  | Order Type Priority Material Availability Check. Allowed values: 5, 10, 15, 20, 25, 99 |
| `icpr_kw_2` | `string` | `varchar` |  |  | Order Type Priority Material Availability Check (keyword). Allowed values: tsmdm.maco.soc, tsmdm.maco.wcs, tsmdm.maco.msc, tsmdm.maco.spc, tsmdm.maco.epp, tsmdm.maco.not.applicable |
| `icpr_3` | `integer` | `int` |  |  | Order Type Priority Material Availability Check. Allowed values: 5, 10, 15, 20, 25, 99 |
| `icpr_kw_3` | `string` | `varchar` |  |  | Order Type Priority Material Availability Check (keyword). Allowed values: tsmdm.maco.soc, tsmdm.maco.wcs, tsmdm.maco.msc, tsmdm.maco.spc, tsmdm.maco.epp, tsmdm.maco.not.applicable |
| `icpr_4` | `integer` | `int` |  |  | Order Type Priority Material Availability Check. Allowed values: 5, 10, 15, 20, 25, 99 |
| `icpr_kw_4` | `string` | `varchar` |  |  | Order Type Priority Material Availability Check (keyword). Allowed values: tsmdm.maco.soc, tsmdm.maco.wcs, tsmdm.maco.msc, tsmdm.maco.spc, tsmdm.maco.epp, tsmdm.maco.not.applicable |
| `icpr_5` | `integer` | `int` |  |  | Order Type Priority Material Availability Check. Allowed values: 5, 10, 15, 20, 25, 99 |
| `icpr_kw_5` | `string` | `varchar` |  |  | Order Type Priority Material Availability Check (keyword). Allowed values: tsmdm.maco.soc, tsmdm.maco.wcs, tsmdm.maco.msc, tsmdm.maco.spc, tsmdm.maco.epp, tsmdm.maco.not.applicable |
| `pmnt` | `integer` | `int` |  |  | Use Preventive Maintenance. Allowed values: 1, 2 |
| `pmnt_kw` | `string` | `varchar` |  |  | Use Preventive Maintenance (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `sbmt` | `integer` | `int` |  |  | Use Subcontract Management. Allowed values: 1, 2 |
| `sbmt_kw` | `string` | `varchar` |  |  | Use Subcontract Management (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `tlgi` | `integer` | `int` |  |  | Tool Requirements Planning. Allowed values: 1, 2 |
| `tlgi_kw` | `string` | `varchar` |  |  | Tool Requirements Planning (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ingr` | `integer` | `int` |  |  | Installation Groups. Allowed values: 1, 2 |
| `ingr_kw` | `string` | `varchar` |  |  | Installation Groups (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ttpl` | `integer` | `int` |  |  | Territory Planning. Allowed values: 1, 2 |
| `ttpl_kw` | `string` | `varchar` |  |  | Territory Planning (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `clgr` | `string` | `varchar` |  |  | List Group Service. Max length: 3 |
| `uwev` | `integer` | `int` |  |  | Use Warranty Events. Allowed values: 1, 2 |
| `uwev_kw` | `string` | `varchar` |  |  | Use Warranty Events (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `isom` | `integer` | `int` |  |  | Items Service by Office is Mandatory. Allowed values: 1, 2 |
| `isom_kw` | `string` | `varchar` |  |  | Items Service by Office is Mandatory (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `cibp` | `integer` | `int` |  |  | Create Item - Service Business Partner After Delivery. Allowed values: 1, 2 |
| `cibp_kw` | `string` | `varchar` |  |  | Create Item - Service Business Partner After Delivery (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `inby` | `integer` | `int` |  |  | Default Value for Invoicing By. Allowed values: 5, 10, 99 |
| `inby_kw` | `string` | `varchar` |  |  | Default Value for Invoicing By (keyword). Allowed values: tcinby.project, tcinby.service, tcinby.not.appl |
| `ngtb` | `string` | `varchar` |  |  | Number Group for Travel Rate Books. Max length: 3 |
| `sntb` | `string` | `varchar` |  |  | Series for Travel Rate Books. Max length: 8 |
| `npeg` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `npeg_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `iwos` | `integer` | `int` |  |  | Initial Warehousing Order Status Determined by Warehousing. Allowed values: 1, 2 |
| `iwos_kw` | `string` | `varchar` |  |  | Initial Warehousing Order Status Determined by Warehousing (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `grpl` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `grpl_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `skla` | `integer` | `int` |  |  | Obsolete. Allowed values: 5, 10 |
| `skla_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tsmdm.skil.mandatory, tsmdm.skil.preferred |
| `sklb` | `integer` | `int` |  |  | Obsolete. Allowed values: 5, 10 |
| `sklb_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tsmdm.skil.mandatory, tsmdm.skil.preferred |
| `sklc` | `integer` | `int` |  |  | Obsolete. Allowed values: 5, 10 |
| `sklc_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tsmdm.skil.mandatory, tsmdm.skil.preferred |
| `ctii` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `ctii_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `kpad` | `integer` | `int` |  |  | Obsolete. Allowed values: 0, 5, 10 |
| `kpad_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: , tsmdm.kpad.itu, tsmdm.kpad.nitu |
| `erdc` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `erdc_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `icsc` | `string` | `varchar` |  |  | Obsolete. Max length: 47 |
| `ipsm` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2, 3 |
| `ipsm_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tsmdm.ipsm.serv.sls.books, tsmdm.ipsm.trans.books, tsmdm.ipsm.serv.items |
| `text` | `integer` | `int` |  |  | Text |
| `undi_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `untm_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `unlr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `untd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `uday_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `uwks_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `umon_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `uyrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs001 Units |
| `tvcc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `ract_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcccp001 Availability Types |
| `clgr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcibd301 List Groups |
| `ngtb_sntb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs050 First Free Numbers |
| `ngtb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs051 Number Groups |
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

# tffam100

LN tffam100 Asset table, Generated 2026-04-10T19:41:31Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tffam100` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `anbr`, `aext` |
| **Column count** | 126 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `anbr` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Number. Max length: 15 |
| `aext` | `string` | `varchar` | `PK` | `not_null` | (required) Asset Extension. Max length: 15 |
| `acmc_1` | `float` | `float` |  |  | Accumulated Maintenance Cost |
| `acmc_2` | `float` | `float` |  |  | Accumulated Maintenance Cost |
| `acmc_3` | `float` | `float` |  |  | Accumulated Maintenance Cost |
| `acmd` | `date` | `date` |  |  | Last Maintenance Date |
| `agrp` | `string` | `varchar` |  |  | Group. Max length: 10 |
| `ascd` | `integer` | `int` |  |  | Asset Source. Allowed values: 1, 2, 3, 4, 5, 10 |
| `ascd_kw` | `string` | `varchar` |  |  | Asset Source (keyword). Allowed values: tffam.ascd.manu, tffam.ascd.ap, tffam.ascd.gl, tffam.ascd.feed, tffam.ascd.cmg, tffam.ascd.rec.asset |
| `auto` | `integer` | `int` |  |  | Automobile. Allowed values: 1, 2 |
| `auto_kw` | `string` | `varchar` |  |  | Automobile (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `bpct` | `float` | `float` |  |  | Business Percentage |
| `bpid` | `string` | `varchar` |  |  | Business Partner. Max length: 9 |
| `c263` | `float` | `float` |  |  | Comment |
| `capi` | `float` | `float` |  |  | Comment |
| `comp` | `integer` | `int` |  |  | Company |
| `cost` | `float` | `float` |  |  | Transaction Cost |
| `ctgy` | `string` | `varchar` |  |  | Category. Max length: 10 |
| `curr` | `string` | `varchar` |  |  | Transaction Currency. Max length: 3 |
| `date` | `date` | `date` |  |  | Date |
| `dbfg` | `integer` | `int` |  |  | Asset Distribution By. Allowed values: 1, 2 |
| `dbfg_kw` | `string` | `varchar` |  |  | Asset Distribution By (keyword). Allowed values: tffam.byqp.quantity, tffam.byqp.percent |
| `desc__en_us` | `string` | `varchar` |  | `not_null` | (required) Description - base language. Max length: 100 |
| `itca` | `float` | `float` |  |  | Current ITC Amount |
| `itcd` | `integer` | `int` |  |  | ITC Method. Allowed values: 1, 2, 3 |
| `itcd_kw` | `string` | `varchar` |  |  | ITC Method (keyword). Allowed values: tffam.itcc.none, tffam.itcc.redu.basi, tffam.itcc.redu.cred |
| `item__en_us` | `string` | `varchar` |  | `not_null` | (required) Purchase Order Part - base language. Max length: 15 |
| `list` | `integer` | `int` |  |  | Listed. Allowed values: 1, 2 |
| `list_kw` | `string` | `varchar` |  |  | Listed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ltdd` | `float` | `float` |  |  | New |
| `newu` | `integer` | `int` |  |  | New. Allowed values: 1, 2 |
| `newu_kw` | `string` | `varchar` |  |  | New (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `oqty` | `integer` | `int` |  |  | Original Quantity |
| `ownc` | `integer` | `int` |  |  | Owner. Allowed values: 1, 2, 3, 4 |
| `ownc_kw` | `string` | `varchar` |  |  | Owner (keyword). Allowed values: tffam.ownc.owne, tffam.ownc.oper.leas, tffam.ownc.capi.leas, tffam.ownc.thir.part |
| `pnbr` | `string` | `varchar` |  |  | Purchase Order. Max length: 20 |
| `prod` | `integer` | `int` |  |  | Period |
| `prcm` | `integer` | `int` |  |  | Project Company |
| `proj` | `string` | `varchar` |  |  | Project. Max length: 9 |
| `cact` | `string` | `varchar` |  |  | Activity. Max length: 16 |
| `cspa` | `string` | `varchar` |  |  | Element. Max length: 16 |
| `pudt` | `date` | `date` |  |  | Purchase Date |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rate_1` | `float` | `float` |  |  | Rate |
| `rate_2` | `float` | `float` |  |  | Rate |
| `rate_3` | `float` | `float` |  |  | Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `rcmp` | `integer` | `int` |  |  | Reference Company |
| `rext` | `string` | `varchar` |  |  | Reference Asset Ext. Max length: 15 |
| `rnbr` | `string` | `varchar` |  |  | Reference Asset Nbr. Max length: 15 |
| `s179` | `float` | `float` |  |  | Section 179 Value |
| `salv` | `float` | `float` |  |  | Salvage Value |
| `sctg` | `string` | `varchar` |  |  | Default Subcategory. Max length: 10 |
| `shft` | `float` | `float` |  |  | Shift Factor |
| `snbr` | `string` | `varchar` |  |  | Serial Number. Max length: 30 |
| `srvc` | `date` | `date` |  |  | In Service Date |
| `stat` | `integer` | `int` |  |  | Status. Allowed values: 1, 2, 3, 4 |
| `stat_kw` | `string` | `varchar` |  |  | Status (keyword). Allowed values: tffam.asta.ente, tffam.asta.capi, tffam.asta.full.disp, tffam.asta.remo |
| `tagn__en_us` | `string` | `varchar` |  | `not_null` | (required) Tag Number - base language. Max length: 15 |
| `tqty` | `integer` | `int` |  |  | Current Quantity |
| `trfg` | `integer` | `int` |  |  | Transferred. Allowed values: 1, 2 |
| `trfg_kw` | `string` | `varchar` |  |  | Transferred (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vinb` | `integer` | `int` |  |  | Is or was vintage. Allowed values: 1, 2 |
| `vinb_kw` | `string` | `varchar` |  |  | Is or was vintage (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `vint` | `string` | `varchar` |  |  | Vintage/Group Account. Max length: 20 |
| `year` | `integer` | `int` |  |  | Year |
| `simu` | `integer` | `int` |  |  | Simulated. Allowed values: 1, 2 |
| `simu_kw` | `string` | `varchar` |  |  | Simulated (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ramt` | `float` | `float` |  |  | Reinvestment Amount |
| `ryer` | `integer` | `int` |  |  | Reinvestment Year |
| `imag` | `string` | `varchar` |  |  | Picture. Max length: 22 |
| `cola` | `integer` | `int` |  |  | Collective Asset. Allowed values: 1, 2 |
| `cola_kw` | `string` | `varchar` |  |  | Collective Asset (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `ratu` | `integer` | `int` |  |  | Register Asset Technical Upgrades. Allowed values: 1, 2 |
| `ratu_kw` | `string` | `varchar` |  |  | Register Asset Technical Upgrades (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `atbc_1` | `float` | `float` |  |  | Amount to be Capitalized |
| `atbc_2` | `float` | `float` |  |  | Amount to be Capitalized |
| `atbc_3` | `float` | `float` |  |  | Amount to be Capitalized |
| `aacq_1` | `float` | `float` |  |  | Additional Acquisition |
| `aacq_2` | `float` | `float` |  |  | Additional Acquisition |
| `aacq_3` | `float` | `float` |  |  | Additional Acquisition |
| `tuam_1` | `float` | `float` |  |  | Technical Upgrade |
| `tuam_2` | `float` | `float` |  |  | Technical Upgrade |
| `tuam_3` | `float` | `float` |  |  | Technical Upgrade |
| `owco` | `integer` | `int` |  |  | Owning Company |
| `ownd` | `string` | `varchar` |  |  | Owning Department. Max length: 6 |
| `ucsh` | `integer` | `int` |  |  | Use Transaction Costs in HC. Allowed values: 1, 2 |
| `ucsh_kw` | `string` | `varchar` |  |  | Use Transaction Costs in HC (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `csth_1` | `float` | `float` |  |  | Transaction Cost in HC |
| `csth_2` | `float` | `float` |  |  | Transaction Cost in HC |
| `csth_3` | `float` | `float` |  |  | Transaction Cost in HC |
| `txta` | `integer` | `int` |  |  | Comment |
| `txtb` | `integer` | `int` |  |  | Assoc. Notes |
| `cdf_aid1__en_us` | `string` | `varchar` |  | `not_null` | (required) IPS ID1 - base language. Max length: 30 |
| `cdf_akey__en_us` | `string` | `varchar` |  | `not_null` | (required) AssetValuationKey - base language. Max length: 30 |
| `cdf_asid__en_us` | `string` | `varchar` |  | `not_null` | (required) IPS ID - base language. Max length: 30 |
| `cdf_atyp` | `string` | `varchar` |  |  | AssetType. Max length: 50 |
| `cdf_htir` | `string` | `varchar` |  |  | Hierarchy Tier. Max length: 50 |
| `cdf_ipst` | `integer` | `int` |  |  | Operational Status. Allowed values: 1, 2, 3, 4, 5, 6, 7 |
| `cdf_ipst_kw` | `string` | `varchar` |  |  | Operational Status (keyword). Allowed values: tfcdf_lst009.ent, tfcdf_lst009.av, tfcdf_lst009.acq, tfcdf_lst009.op, tfcdf_lst009.out, tfcdf_lst009.disp, tfcdf_lst009.ab |
| `cdf_utyp` | `string` | `varchar` |  |  | UnitType. Max length: 50 |
| `agrp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam400 Asset Groups |
| `bpid_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ctgy_sctg_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam220 Subcategory |
| `ctgy_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam200 Categories |
| `curr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `vint_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tffam770 Vintage/Group Account |
| `owco_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcemm170 Companies |
| `ownd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs065 Departments |
| `txta_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `cdf_atyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table txfam001 Asset Types |
| `cdf_utyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table txfam002 Unit Types |
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

# ttaad200

LN ttaad200 User Data table, Generated 2026-04-10T19:42:39Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_ttaad200` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `user` |
| **Column count** | 136 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `user` | `string` | `varchar` | `PK` | `not_null` | (required) User. Max length: 12 |
| `name__en_us` | `string` | `varchar` |  | `not_null` | (required) Name - base language. Max length: 100 |
| `uusr` | `string` | `varchar` |  |  | System Login. Max length: 255 |
| `utyp` | `integer` | `int` |  |  | User Type. Allowed values: 1, 2 |
| `utyp_kw` | `string` | `varchar` |  |  | User Type (keyword). Allowed values: ttaad.utyp.superuser, ttaad.utyp.normaluser |
| `shtp` | `integer` | `int` |  |  | * Obsolete *. Allowed values: 1, 2, 3 |
| `shtp_kw` | `string` | `varchar` |  |  | * Obsolete * (keyword). Allowed values: ttaad.shtp.no, ttaad.shtp.unix, ttaad.shtp.session |
| `shcm` | `string` | `varchar` |  |  | * Obsolete *. Max length: 20 |
| `sttm` | `integer` | `int` |  |  | * Obsolete * |
| `edtm` | `integer` | `int` |  |  | * Obsolete * |
| `hist` | `integer` | `int` |  |  | * Obsolete *. Allowed values: 1, 2 |
| `hist_kw` | `string` | `varchar` |  |  | * Obsolete * (keyword). Allowed values: ttyeno.yes, ttyeno.no |
| `pwyn` | `integer` | `int` |  |  | * Obsolete *. Allowed values: 1, 2 |
| `pwyn_kw` | `string` | `varchar` |  |  | * Obsolete * (keyword). Allowed values: ttyeno.yes, ttyeno.no |
| `pswd` | `string` | `varchar` |  |  | Worklist Handler. Max length: 512 |
| `wait` | `integer` | `int` |  |  | Version |
| `delh` | `integer` | `int` |  |  | BW Startup Command |
| `prrt` | `integer` | `int` |  |  | Print Priority |
| `uids` | `integer` | `int` |  |  | User Interface. Allowed values: 1, 2 |
| `uids_kw` | `string` | `varchar` |  |  | User Interface (keyword). Allowed values: ttaad.uids.ascii, ttaad.uids.xwindows11.4 |
| `pacc` | `string` | `varchar` |  |  | Package Combination. Max length: 8 |
| `comp` | `integer` | `int` |  |  | Company |
| `clan` | `string` | `varchar` |  |  | Language. Max length: 1 |
| `cpac` | `string` | `varchar` |  |  | Start-Up Menu (Package). Max length: 2 |
| `cmod` | `string` | `varchar` |  |  | Start-Up Menu (Module). Max length: 3 |
| `cmen` | `string` | `varchar` |  |  | Start-Up Menu (Menu Code). Max length: 8 |
| `maxp` | `integer` | `int` |  |  | * Obsolete * |
| `intt` | `integer` | `int` |  |  | Application Toolbar (Text) |
| `csbn` | `integer` | `int` |  |  | Menu Browser. Allowed values: 1, 2 |
| `csbn_kw` | `string` | `varchar` |  |  | Menu Browser (keyword). Allowed values: ttyeno.yes, ttyeno.no |
| `csbh` | `integer` | `int` |  |  | Infor LN Process Browser (DEM). Allowed values: 1, 2 |
| `csbh_kw` | `string` | `varchar` |  |  | Infor LN Process Browser (DEM) (keyword). Allowed values: ttyeno.yes, ttyeno.no |
| `palp` | `integer` | `int` |  |  | Infor LN Workflow. Allowed values: 1, 2 |
| `palp_kw` | `string` | `varchar` |  |  | Infor LN Workflow (keyword). Allowed values: ttyeno.yes, ttyeno.no |
| `loca` | `string` | `varchar` |  |  | * Obsolete *. Max length: 14 |
| `stpr` | `integer` | `int` |  |  | Startup Program. Allowed values: 2, 4, 5, 6, 7, 8 |
| `stpr_kw` | `string` | `varchar` |  |  | Startup Program (keyword). Allowed values: ttaad.stpr.menu.browser, ttaad.stpr.desktop, ttaad.stpr.org.browser, ttaad.stpr.org.workflow, ttaad.stpr.bi.browser, ttaad.stpr.worktop |
| `dskt` | `string` | `varchar` |  |  | Startup Desktop Name. Max length: 20 |
| `stdp` | `integer` | `int` |  |  | Standard Program Type. Allowed values: 1, 2 |
| `stdp_kw` | `string` | `varchar` |  |  | Standard Program Type (keyword). Allowed values: ttaad.stdp.ascii, ttaad.stdp.graphical |
| `tdir` | `integer` | `int` |  |  | * Obsolete *. Allowed values: 0, 1, 2 |
| `tdir_kw` | `string` | `varchar` |  |  | * Obsolete * (keyword). Allowed values: , ttaad.tdir.ltor, ttaad.tdir.rtol |
| `apsv` | `string` | `varchar` |  |  | * Obsolete *. Max length: 60 |
| `syst` | `string` | `varchar` |  |  | * Obsolete *. Max length: 80 |
| `s132` | `integer` | `int` |  |  | Remember the user's settings. Allowed values: 0, 1, 2 |
| `s132_kw` | `string` | `varchar` |  |  | Remember the user's settings (keyword). Allowed values: , ttyeno.yes, ttyeno.no |
| `uoos` | `integer` | `int` |  |  | * Obsolete *. Allowed values: 0, 1, 2 |
| `uoos_kw` | `string` | `varchar` |  |  | * Obsolete * (keyword). Allowed values: , ttyeno.yes, ttyeno.no |
| `scdh` | `integer` | `int` |  |  | * Obsolete *. Allowed values: 0, 1, 2 |
| `scdh_kw` | `string` | `varchar` |  |  | * Obsolete * (keyword). Allowed values: , ttyeno.yes, ttyeno.no |
| `syst2` | `string` | `varchar` |  |  | * Obsolete *. Max length: 80 |
| `crol_1` | `string` | `varchar` |  |  | Role. Max length: 13 |
| `crol_2` | `string` | `varchar` |  |  | Role. Max length: 13 |
| `crol_3` | `string` | `varchar` |  |  | Role. Max length: 13 |
| `crol_4` | `string` | `varchar` |  |  | Role. Max length: 13 |
| `crol_5` | `string` | `varchar` |  |  | Role. Max length: 13 |
| `crol_6` | `string` | `varchar` |  |  | Role. Max length: 13 |
| `tusd` | `string` | `varchar` |  |  | User Data. Max length: 13 |
| `ttxg` | `string` | `varchar` |  |  | Default Text Groups. Max length: 13 |
| `ttxf` | `string` | `varchar` |  |  | Default Text Fields. Max length: 13 |
| `ttxa` | `string` | `varchar` |  |  | Text Group Authorizations. Max length: 13 |
| `tter` | `string` | `varchar` |  |  | Terminal Authorizations. Max length: 13 |
| `tdvc` | `string` | `varchar` |  |  | Device Preferences. Max length: 13 |
| `tdvp` | `string` | `varchar` |  |  | Development Parameters. Max length: 13 |
| `tdva` | `string` | `varchar` |  |  | Developer Authorizations. Max length: 13 |
| `allp` | `integer` | `int` |  |  | Default Authorization for all Package VRCs. Allowed values: 0, 1, 2 |
| `allp_kw` | `string` | `varchar` |  |  | Default Authorization for all Package VRCs (keyword). Allowed values: , ttyeno.yes, ttyeno.no |
| `apud` | `integer` | `int` |  |  | Apply User Data. Allowed values: 1, 2 |
| `apud_kw` | `string` | `varchar` |  |  | Apply User Data (keyword). Allowed values: ttyeno.yes, ttyeno.no |
| `whlp` | `string` | `varchar` |  |  | * Obsolete *. Max length: 8 |
| `phlp__en_us` | `string` | `varchar` |  | `not_null` | (required) Path to Help Files / Help Server - base language. Max length: 100 |
| `tlb1` | `integer` | `int` |  |  | Standard Toolbar. Allowed values: 1, 2, 3, 4 |
| `tlb1_kw` | `string` | `varchar` |  |  | Standard Toolbar (keyword). Allowed values: ttadv.tlbpos.top, ttadv.tlbpos.bottom, ttadv.tlbpos.left, ttadv.tlbpos.right |
| `tlb2` | `integer` | `int` |  |  | Application Toolbar (Icons). Allowed values: 1, 2, 3, 4 |
| `tlb2_kw` | `string` | `varchar` |  |  | Application Toolbar (Icons) (keyword). Allowed values: ttadv.tlbpos.top, ttadv.tlbpos.bottom, ttadv.tlbpos.left, ttadv.tlbpos.right |
| `uico` | `integer` | `int` |  |  | UI Configuration. Allowed values: 1, 2 |
| `uico_kw` | `string` | `varchar` |  |  | UI Configuration (keyword). Allowed values: ttuico.page, ttuico.field |
| `wtit__en_us` | `string` | `varchar` |  | `not_null` | (required) Format of window title - base language. Max length: 70 |
| `z_mmode` | `integer` | `int` |  |  | Message Mode. Allowed values: 1, 2, 3 |
| `z_mmode_kw` | `string` | `varchar` |  |  | Message Mode (keyword). Allowed values: ttams.mmode.interactive, ttams.mmode.non.interrupt, ttams.mmode.page.mode |
| `z_pswd` | `string` | `varchar` |  |  | Authorization Password. Max length: 512 |
| `za_dlan` | `string` | `varchar` |  |  | Data Language. Max length: 5 |
| `zb_ssou__en_us` | `string` | `varchar` |  | `not_null` | (required) SSO User - base language. Max length: 132 |
| `zb_ugsu` | `integer` | `int` |  |  | Use Generic System User. Allowed values: 1, 2 |
| `zb_ugsu_kw` | `string` | `varchar` |  |  | Use Generic System User (keyword). Allowed values: ttyeno.yes, ttyeno.no |
| `zc_fcom` | `integer` | `int` |  |  | Financial Company |
| `zc_lcom` | `integer` | `int` |  |  | Logistical Company |
| `zd_idnt` | `string` | `varchar` |  |  | Identity (UUID). Max length: 36 |
| `role` | `string` | `varchar` |  |  | Role ID. Max length: 20 |
| `ifsu` | `integer` | `int` |  |  | IFS User. Allowed values: 1, 2 |
| `ifsu_kw` | `string` | `varchar` |  |  | IFS User (keyword). Allowed values: ttyeno.yes, ttyeno.no |
| `ifss` | `integer` | `int` |  |  | IFS Status. Allowed values: 5, 10, 15, 99 |
| `ifss_kw` | `string` | `varchar` |  |  | IFS Status (keyword). Allowed values: ttaad.ifss.enabled, ttaad.ifss.disabled, ttaad.ifss.removed, ttaad.ifss.not.applicable |
| `ifsi__en_us` | `string` | `varchar` |  | `not_null` | (required) IFS User ID - base language. Max length: 132 |
| `mail__en_us` | `string` | `varchar` |  | `not_null` | (required) Email Address - base language. Max length: 100 |
| `mtyp` | `integer` | `int` |  |  | Email Type. Allowed values: 1, 2, 3, 4, 5, 6, 7 |
| `mtyp_kw` | `string` | `varchar` |  |  | Email Type (keyword). Allowed values: ttcmf.etyp.smtp, ttcmf.etyp.x400, ttcmf.etyp.notes, ttcmf.etyp.ccmail, ttcmf.etyp.mhs, ttcmf.etyp.ms, ttcmf.etyp.none |
| `faxn` | `string` | `varchar` |  |  | Fax Number. Max length: 15 |
| `telx` | `string` | `varchar` |  |  | Telex Number. Max length: 15 |
| `sita` | `string` | `varchar` |  |  | SITA Address. Max length: 7 |
| `smsa` | `string` | `varchar` |  |  | SMS Address. Max length: 50 |
| `defa` | `integer` | `int` |  |  | Default Address Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8 |
| `defa_kw` | `string` | `varchar` |  |  | Default Address Type (keyword). Allowed values: ttcmf.defa.email, ttcmf.defa.fax, ttcmf.defa.telex, ttcmf.defa.sita, ttcmf.defa.sms, ttcmf.defa.telephone, ttcmf.defa.pager, ttcmf.defa.none |
| `tele` | `string` | `varchar` |  |  | Telephone Number. Max length: 15 |
| `ucln` | `integer` | `int` |  |  | Use Browser Language. Allowed values: 1, 2 |
| `ucln_kw` | `string` | `varchar` |  |  | Use Browser Language (keyword). Allowed values: ttyeno.yes, ttyeno.no |
| `uspr` | `string` | `varchar` |  |  | User Profile. Max length: 13 |
| `uhmd` | `integer` | `int` |  |  | Use Other Home Directory. Allowed values: 1, 2 |
| `uhmd_kw` | `string` | `varchar` |  |  | Use Other Home Directory (keyword). Allowed values: ttyeno.yes, ttyeno.no |
| `hmdi__en_us` | `string` | `varchar` |  | `not_null` | (required) Home Directory - base language. Max length: 100 |
| `crdt` | `timestamp` | `timestamp_ntz` |  |  | Creation Date |
| `crus` | `string` | `varchar` |  |  | Created by. Max length: 12 |
| `ifsc` | `string` | `varchar` |  |  | Created by IFS ID. Max length: 36 |
| `updt` | `timestamp` | `timestamp_ntz` |  |  | Modified Date |
| `upus` | `string` | `varchar` |  |  | Modified by. Max length: 12 |
| `ifsm` | `string` | `varchar` |  |  | Modified by IFS ID. Max length: 36 |
| `user_role_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ttaad204 User Roles |
| `pacc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ttaad120 Package Combinations |
| `comp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ttaad100 Companies |
| `clan_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ttaad110 Languages |
| `cpac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ttadv100 Packages |
| `loca_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttss002 Locale Information |
| `tusd_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ttams110 User Data Template |
| `tdvp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ttams150 Development Parameters Template |
| `uspr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table ttams160 User Profile Template |
| `tenant` | `string` | `varchar` |  |  | Max length: 22 |
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

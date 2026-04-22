{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5GROUPS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['UGR_LASTSAVED'], type='timestamp_ntz', alias='ugr_lastsaved') }},
        {{ trx_json_extract('src', ['UGR_MOBILEAPP'], type='varchar', alias='ugr_mobileapp') }},
        {{ trx_json_extract('src', ['UGR_CLASS'], type='varchar', alias='ugr_class') }},
        {{ trx_json_extract('src', ['UGR_MRC'], type='varchar', alias='ugr_mrc') }},
        {{ trx_json_extract('src', ['UGR_PRINTER'], type='varchar', alias='ugr_printer') }},
        {{ trx_json_extract('src', ['UGR_CORRBOOK'], type='varchar', alias='ugr_corrbook') }},
        {{ trx_json_extract('src', ['UGR_CLASS_ORG'], type='varchar', alias='ugr_class_org') }},
        {{ trx_json_extract('src', ['UGR_LOGIN'], type='varchar', alias='ugr_login') }},
        {{ trx_json_extract('src', ['UGR_REQUESTOR'], type='varchar', alias='ugr_requestor') }},
        {{ trx_json_extract('src', ['UGR_UPDATECOUNT'], type='float', alias='ugr_updatecount') }},
        {{ trx_json_extract('src', ['UGR_CREATED'], type='timestamp_ntz', alias='ugr_created') }},
        {{ trx_json_extract('src', ['UGR_UPDATED'], type='timestamp_ntz', alias='ugr_updated') }},
        {{ trx_json_extract('src', ['UGR_SESSIONTIMEOUT'], type='float', alias='ugr_sessiontimeout') }},
        {{ trx_json_extract('src', ['UGR_JOBTYPE'], type='varchar', alias='ugr_jobtype') }},
        {{ trx_json_extract('src', ['UGR_ADDPERM'], type='varchar', alias='ugr_addperm') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADWORKORDERS'], type='varchar', alias='ugr_downloadworkorders') }},
        {{ trx_json_extract('src', ['UGR_FORDATASPY'], type='float', alias='ugr_fordataspy') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADEMPLOYEE'], type='varchar', alias='ugr_downloademployee') }},
        {{ trx_json_extract('src', ['UGR_FORTRADE'], type='varchar', alias='ugr_fortrade') }},
        {{ trx_json_extract('src', ['UGR_FORDEPARTMENT_EMP'], type='varchar', alias='ugr_fordepartment_emp') }},
        {{ trx_json_extract('src', ['UGR_FOREMPLOYEE'], type='varchar', alias='ugr_foremployee') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADSTANDARDWOS'], type='varchar', alias='ugr_downloadstandardwos') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADINSPRESULTS'], type='varchar', alias='ugr_downloadinspresults') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADTASK'], type='varchar', alias='ugr_downloadtask') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADEQUIPMENT'], type='varchar', alias='ugr_downloadequipment') }},
        {{ trx_json_extract('src', ['UGR_FORDEPARTMENT_EQUIP'], type='varchar', alias='ugr_fordepartment_equip') }},
        {{ trx_json_extract('src', ['UGR_FORLOCATION'], type='varchar', alias='ugr_forlocation') }},
        {{ trx_json_extract('src', ['UGR_FORCLASS_EQUIP'], type='varchar', alias='ugr_forclass_equip') }},
        {{ trx_json_extract('src', ['UGR_FORCATEGORY'], type='varchar', alias='ugr_forcategory') }},
        {{ trx_json_extract('src', ['UGR_FOREQUIPMENT'], type='varchar', alias='ugr_forequipment') }},
        {{ trx_json_extract('src', ['UGR_FORTYPE'], type='varchar', alias='ugr_fortype') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADEQUIPCOMMENTS'], type='varchar', alias='ugr_downloadequipcomments') }},
        {{ trx_json_extract('src', ['UGR_FORDOWNLOADEDWO_EQCMT'], type='varchar', alias='ugr_fordownloadedwo_eqcmt') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADEQUIPHISTORY'], type='varchar', alias='ugr_downloadequiphistory') }},
        {{ trx_json_extract('src', ['UGR_FORDOWNLOADEDWO_EQHST'], type='varchar', alias='ugr_fordownloadedwo_eqhst') }},
        {{ trx_json_extract('src', ['UGR_FORLATESTDAYS'], type='float', alias='ugr_forlatestdays') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADEQCUSTOMFIELDS'], type='varchar', alias='ugr_downloadeqcustomfields') }},
        {{ trx_json_extract('src', ['UGR_FORDOWNLOADEDWO_EQCF'], type='varchar', alias='ugr_fordownloadedwo_eqcf') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADCOSTCODES'], type='varchar', alias='ugr_downloadcostcodes') }},
        {{ trx_json_extract('src', ['UGR_FORCLASS_CC'], type='varchar', alias='ugr_forclass_cc') }},
        {{ trx_json_extract('src', ['UGR_FORCOSTCODE'], type='varchar', alias='ugr_forcostcode') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADEQHISTCOMMENTS'], type='varchar', alias='ugr_downloadeqhistcomments') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADSTORES'], type='varchar', alias='ugr_downloadstores') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADPARTS'], type='varchar', alias='ugr_downloadparts') }},
        {{ trx_json_extract('src', ['UGR_FORSTORE_PART'], type='varchar', alias='ugr_forstore_part') }},
        {{ trx_json_extract('src', ['UGR_FORPART_PART'], type='varchar', alias='ugr_forpart_part') }},
        {{ trx_json_extract('src', ['UGR_MAXINSTOCKANDQUANTITY'], type='float', alias='ugr_maxinstockandquantity') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADBINS'], type='varchar', alias='ugr_downloadbins') }},
        {{ trx_json_extract('src', ['UGR_FORSTORE_BIN'], type='varchar', alias='ugr_forstore_bin') }},
        {{ trx_json_extract('src', ['UGR_FORBIN'], type='varchar', alias='ugr_forbin') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADPHYINVENTORY'], type='varchar', alias='ugr_downloadphyinventory') }},
        {{ trx_json_extract('src', ['UGR_FORSTORE_INV'], type='varchar', alias='ugr_forstore_inv') }},
        {{ trx_json_extract('src', ['UGR_FORLINE'], type='varchar', alias='ugr_forline') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADSUPPLIER'], type='varchar', alias='ugr_downloadsupplier') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADASSETINVENTORY'], type='varchar', alias='ugr_downloadassetinventory') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADSTOCK'], type='varchar', alias='ugr_downloadstock') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADESIGNSETTINGS'], type='varchar', alias='ugr_downloadesignsettings') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADMTRREADING'], type='varchar', alias='ugr_downloadmtrreading') }},
        {{ trx_json_extract('src', ['UGR_DOWNLOADDOCPRINTWO'], type='varchar', alias='ugr_downloaddocprintwo') }},
        {{ trx_json_extract('src', ['UGR_EQUIPFORDATASPY'], type='float', alias='ugr_equipfordataspy') }},
        {{ trx_json_extract('src', ['UGR_MTRREADINGLATESTDAYS'], type='float', alias='ugr_mtrreadinglatestdays') }},
        {{ trx_json_extract('src', ['UGR_CODE'], type='varchar', alias='ugr_code') }},
        {{ trx_json_extract('src', ['UGR_DESC'], type='varchar', alias='ugr_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5groups') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ugr_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ugr_lastsaved']) }}

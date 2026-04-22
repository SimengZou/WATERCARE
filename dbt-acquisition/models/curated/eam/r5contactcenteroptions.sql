{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CONTACTCENTEROPTIONS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['COP_LASTSAVED'], type='timestamp_ntz', alias='cop_lastsaved') }},
        {{ trx_json_extract('src', ['COP_INCLUDEPMWORKORDERS'], type='varchar', alias='cop_includepmworkorders') }},
        {{ trx_json_extract('src', ['COP_DEFCONTACTSOURCE'], type='varchar', alias='cop_defcontactsource') }},
        {{ trx_json_extract('src', ['COP_DEFWOSTATUS'], type='varchar', alias='cop_defwostatus') }},
        {{ trx_json_extract('src', ['COP_DEFOPENSTATUS'], type='varchar', alias='cop_defopenstatus') }},
        {{ trx_json_extract('src', ['COP_DEFFOLLOWUPSTATUS'], type='varchar', alias='cop_deffollowupstatus') }},
        {{ trx_json_extract('src', ['COP_TOPTENLOOKBACK'], type='float', alias='cop_toptenlookback') }},
        {{ trx_json_extract('src', ['COP_CLOSEABLE'], type='varchar', alias='cop_closeable') }},
        {{ trx_json_extract('src', ['COP_COPYCUSTOMERINFO'], type='varchar', alias='cop_copycustomerinfo') }},
        {{ trx_json_extract('src', ['COP_WOCLOSEDAYS'], type='float', alias='cop_woclosedays') }},
        {{ trx_json_extract('src', ['COP_DEFTYPE'], type='varchar', alias='cop_deftype') }},
        {{ trx_json_extract('src', ['COP_DEFFINDBY'], type='varchar', alias='cop_deffindby') }},
        {{ trx_json_extract('src', ['COP_UPDATED'], type='timestamp_ntz', alias='cop_updated') }},
        {{ trx_json_extract('src', ['COP_UPDATEDBY'], type='varchar', alias='cop_updatedby') }},
        {{ trx_json_extract('src', ['COP_HIGHLIGHTMAP'], type='varchar', alias='cop_highlightmap') }},
        {{ trx_json_extract('src', ['COP_IDEFEATURE'], type='varchar', alias='cop_idefeature') }},
        {{ trx_json_extract('src', ['COP_SHOWCHILDREN'], type='varchar', alias='cop_showchildren') }},
        {{ trx_json_extract('src', ['COP_NEARADDRESS'], type='varchar', alias='cop_nearaddress') }},
        {{ trx_json_extract('src', ['COP_SCHEDULEWO'], type='varchar', alias='cop_schedulewo') }},
        {{ trx_json_extract('src', ['COP_SHOWPROVIDER'], type='varchar', alias='cop_showprovider') }},
        {{ trx_json_extract('src', ['COP_SHOWSERVICECATEGORY'], type='varchar', alias='cop_showservicecategory') }},
        {{ trx_json_extract('src', ['COP_KBRESULTSPERPAGE'], type='float', alias='cop_kbresultsperpage') }},
        {{ trx_json_extract('src', ['COP_UPDATECOUNT'], type='float', alias='cop_updatecount') }},
        {{ trx_json_extract('src', ['COP_NEWCUSTOMERALLOWED'], type='varchar', alias='cop_newcustomerallowed') }},
        {{ trx_json_extract('src', ['COP_MINPENALTY'], type='float', alias='cop_minpenalty') }},
        {{ trx_json_extract('src', ['COP_DEPTSTRUCTUREUSAGE'], type='varchar', alias='cop_deptstructureusage') }},
        {{ trx_json_extract('src', ['COP_DEFORGUSAGE'], type='varchar', alias='cop_deforgusage') }},
        {{ trx_json_extract('src', ['COP_SPCVALIDATION'], type='varchar', alias='cop_spcvalidation') }},
        {{ trx_json_extract('src', ['COP_EVENTTYPEFILTER'], type='varchar', alias='cop_eventtypefilter') }},
        {{ trx_json_extract('src', ['COP_WODUPLICATECHECK'], type='varchar', alias='cop_woduplicatecheck') }},
        {{ trx_json_extract('src', ['COP_WOOPENDAYS'], type='float', alias='cop_woopendays') }},
        {{ trx_json_extract('src', ['COP_WOTYPES'], type='varchar', alias='cop_wotypes') }},
        {{ trx_json_extract('src', ['COP_WOSTATUSES'], type='varchar', alias='cop_wostatuses') }},
        {{ trx_json_extract('src', ['COP_WOHEADEROBJONLY'], type='varchar', alias='cop_woheaderobjonly') }},
        {{ trx_json_extract('src', ['COP_MATCHSC'], type='varchar', alias='cop_matchsc') }},
        {{ trx_json_extract('src', ['COP_MATCHSPC'], type='varchar', alias='cop_matchspc') }},
        {{ trx_json_extract('src', ['COP_SHOWSPC'], type='varchar', alias='cop_showspc') }},
        {{ trx_json_extract('src', ['COP_SHOWSUPPLIER'], type='varchar', alias='cop_showsupplier') }},
        {{ trx_json_extract('src', ['COP_CHKDUPLICATEOPENREQ'], type='varchar', alias='cop_chkduplicateopenreq') }},
        {{ trx_json_extract('src', ['COP_CHKRECURRINGCLOSEDREQ'], type='varchar', alias='cop_chkrecurringclosedreq') }},
        {{ trx_json_extract('src', ['COP_RECURRINGREQLOOKBACKDAYS'], type='float', alias='cop_recurringreqlookbackdays') }},
        {{ trx_json_extract('src', ['COP_DEFPORTALSOURCE'], type='varchar', alias='cop_defportalsource') }},
        {{ trx_json_extract('src', ['COP_DEFPORTALTYPE'], type='varchar', alias='cop_defportaltype') }},
        {{ trx_json_extract('src', ['COP_DEFPORTALSTATUS'], type='varchar', alias='cop_defportalstatus') }},
        {{ trx_json_extract('src', ['COP_ORG'], type='varchar', alias='cop_org') }},
        {{ trx_json_extract('src', ['COP_DEFWOORG'], type='varchar', alias='cop_defwoorg') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5contactcenteroptions') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cop_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['cop_lastsaved']) }}

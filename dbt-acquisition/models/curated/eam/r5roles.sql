{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ROLES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ROL_LASTSAVED'], type='timestamp_ntz', alias='rol_lastsaved') }},
        {{ trx_json_extract('src', ['ROL_GROUP'], type='varchar', alias='rol_group') }},
        {{ trx_json_extract('src', ['ROL_DEFAULTORG'], type='varchar', alias='rol_defaultorg') }},
        {{ trx_json_extract('src', ['ROL_LANG'], type='varchar', alias='rol_lang') }},
        {{ trx_json_extract('src', ['ROL_SUCCESSMSGTIMEOUT'], type='float', alias='rol_successmsgtimeout') }},
        {{ trx_json_extract('src', ['ROL_LOCALE'], type='varchar', alias='rol_locale') }},
        {{ trx_json_extract('src', ['ROL_MRC'], type='varchar', alias='rol_mrc') }},
        {{ trx_json_extract('src', ['ROL_FIRSTFUNC'], type='varchar', alias='rol_firstfunc') }},
        {{ trx_json_extract('src', ['ROL_EWSFIRSTFUNC'], type='varchar', alias='rol_ewsfirstfunc') }},
        {{ trx_json_extract('src', ['ROL_REQAPPVLIMIT'], type='float', alias='rol_reqappvlimit') }},
        {{ trx_json_extract('src', ['ROL_REQAUTHAPPVLIMIT'], type='float', alias='rol_reqauthappvlimit') }},
        {{ trx_json_extract('src', ['ROL_INVAPPVLIMIT'], type='float', alias='rol_invappvlimit') }},
        {{ trx_json_extract('src', ['ROL_INVAPPVLIMITNONPO'], type='float', alias='rol_invappvlimitnonpo') }},
        {{ trx_json_extract('src', ['ROL_PORDAPPVLIMIT'], type='float', alias='rol_pordappvlimit') }},
        {{ trx_json_extract('src', ['ROL_PORDAUTHAPPVLIMIT'], type='float', alias='rol_pordauthappvlimit') }},
        {{ trx_json_extract('src', ['ROL_PICAPPVLIMIT'], type='float', alias='rol_picappvlimit') }},
        {{ trx_json_extract('src', ['ROL_BUYER'], type='varchar', alias='rol_buyer') }},
        {{ trx_json_extract('src', ['ROL_APPROVER'], type='varchar', alias='rol_approver') }},
        {{ trx_json_extract('src', ['ROL_REQUESTOR'], type='varchar', alias='rol_requestor') }},
        {{ trx_json_extract('src', ['ROL_ROUTER'], type='varchar', alias='rol_router') }},
        {{ trx_json_extract('src', ['ROL_UPDATECOUNT'], type='float', alias='rol_updatecount') }},
        {{ trx_json_extract('src', ['ROL_ACTIVE'], type='varchar', alias='rol_active') }},
        {{ trx_json_extract('src', ['ROL_MOBILE'], type='varchar', alias='rol_mobile') }},
        {{ trx_json_extract('src', ['ROL_CONNECTOR'], type='varchar', alias='rol_connector') }},
        {{ trx_json_extract('src', ['ROL_BARCODE'], type='varchar', alias='rol_barcode') }},
        {{ trx_json_extract('src', ['ROL_ANALYTICS'], type='varchar', alias='rol_analytics') }},
        {{ trx_json_extract('src', ['ROL_ADVREPAUTHOR'], type='varchar', alias='rol_advrepauthor') }},
        {{ trx_json_extract('src', ['ROL_ADVREPCONSUMER'], type='varchar', alias='rol_advrepconsumer') }},
        {{ trx_json_extract('src', ['ROL_MOBILEADM'], type='varchar', alias='rol_mobileadm') }},
        {{ trx_json_extract('src', ['ROL_CODE'], type='varchar', alias='rol_code') }},
        {{ trx_json_extract('src', ['ROL_DESC'], type='varchar', alias='rol_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5roles') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rol_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rol_lastsaved']) }}

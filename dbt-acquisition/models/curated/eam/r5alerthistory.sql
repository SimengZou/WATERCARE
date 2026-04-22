{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ALERTHISTORY',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ALH_LASTSAVED'], type='timestamp_ntz', alias='alh_lastsaved') }},
        {{ trx_json_extract('src', ['ALH_RSTATUS'], type='varchar', alias='alh_rstatus') }},
        {{ trx_json_extract('src', ['ALH_RTYPE'], type='varchar', alias='alh_rtype') }},
        {{ trx_json_extract('src', ['ALH_ENTITYCODE'], type='varchar', alias='alh_entitycode') }},
        {{ trx_json_extract('src', ['ALH_ENTITYORG'], type='varchar', alias='alh_entityorg') }},
        {{ trx_json_extract('src', ['ALH_RESULTCODE'], type='varchar', alias='alh_resultcode') }},
        {{ trx_json_extract('src', ['ALH_RESULTORG'], type='varchar', alias='alh_resultorg') }},
        {{ trx_json_extract('src', ['ALH_ERRORMESSAGE'], type='varchar', alias='alh_errormessage') }},
        {{ trx_json_extract('src', ['ALH_CREATED'], type='timestamp_ntz', alias='alh_created') }},
        {{ trx_json_extract('src', ['ALH_UPDATECOUNT'], type='float', alias='alh_updatecount') }},
        {{ trx_json_extract('src', ['ALH_ALERT'], type='varchar', alias='alh_alert') }},
        {{ trx_json_extract('src', ['ALH_TEMPLATE'], type='varchar', alias='alh_template') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5alerthistory') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['alh_lastsaved']) }}

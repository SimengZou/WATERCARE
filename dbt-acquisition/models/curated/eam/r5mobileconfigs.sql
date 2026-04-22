{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MOBILECONFIGS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MCF_LASTSAVED'], type='timestamp_ntz', alias='mcf_lastsaved') }},
        {{ trx_json_extract('src', ['MCF_RCODE'], type='varchar', alias='mcf_rcode') }},
        {{ trx_json_extract('src', ['MCF_CONFIG'], type='float', alias='mcf_config') }},
        {{ trx_json_extract('src', ['MCF_DESK'], type='varchar', alias='mcf_desk') }},
        {{ trx_json_extract('src', ['MCF_COMPTYPE'], type='varchar', alias='mcf_comptype') }},
        {{ trx_json_extract('src', ['MCF_GROUP'], type='varchar', alias='mcf_group') }},
        {{ trx_json_extract('src', ['MCF_XMLCONFIG'], type='varchar', alias='mcf_xmlconfig') }},
        {{ trx_json_extract('src', ['MCF_CREATED'], type='timestamp_ntz', alias='mcf_created') }},
        {{ trx_json_extract('src', ['MCF_UPDATED'], type='timestamp_ntz', alias='mcf_updated') }},
        {{ trx_json_extract('src', ['MCF_UPDATECOUNT'], type='float', alias='mcf_updatecount') }},
        {{ trx_json_extract('src', ['MCF_CODE'], type='varchar', alias='mcf_code') }},
        {{ trx_json_extract('src', ['MCF_USER'], type='varchar', alias='mcf_user') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mobileconfigs') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['mcf_code', 'mcf_rcode', 'mcf_config', 'mcf_desk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['mcf_lastsaved']) }}

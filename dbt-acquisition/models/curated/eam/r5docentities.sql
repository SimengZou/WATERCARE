{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DOCENTITIES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DAE_LASTSAVED'], type='timestamp_ntz', alias='dae_lastsaved') }},
        {{ trx_json_extract('src', ['DAE_ENTITY'], type='varchar', alias='dae_entity') }},
        {{ trx_json_extract('src', ['DAE_RENTITY'], type='varchar', alias='dae_rentity') }},
        {{ trx_json_extract('src', ['DAE_TYPE'], type='varchar', alias='dae_type') }},
        {{ trx_json_extract('src', ['DAE_RTYPE'], type='varchar', alias='dae_rtype') }},
        {{ trx_json_extract('src', ['DAE_CODE'], type='varchar', alias='dae_code') }},
        {{ trx_json_extract('src', ['DAE_IDMCOPY'], type='varchar', alias='dae_idmcopy') }},
        {{ trx_json_extract('src', ['DAE_PRINTONWO'], type='varchar', alias='dae_printonwo') }},
        {{ trx_json_extract('src', ['DAE_COPYTOPO'], type='varchar', alias='dae_copytopo') }},
        {{ trx_json_extract('src', ['DAE_PRINTONPO'], type='varchar', alias='dae_printonpo') }},
        {{ trx_json_extract('src', ['DAE_UPDATECOUNT'], type='float', alias='dae_updatecount') }},
        {{ trx_json_extract('src', ['DAE_CREATECOPYTOWO'], type='varchar', alias='dae_createcopytowo') }},
        {{ trx_json_extract('src', ['DAE_DOCUMENT'], type='varchar', alias='dae_document') }},
        {{ trx_json_extract('src', ['DAE_COPYTOWO'], type='varchar', alias='dae_copytowo') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5docentities') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['dae_document', 'dae_code', 'dae_rentity']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['dae_lastsaved']) }}

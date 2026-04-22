{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5BVDWMROINVPARTSTOBYSTORE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ZDT_KEY'], type='float', alias='zdt_key') }},
        {{ trx_json_extract('src', ['ZOR_KEY'], type='float', alias='zor_key') }},
        {{ trx_json_extract('src', ['ZSO_KEY'], type='float', alias='zso_key') }},
        {{ trx_json_extract('src', ['MRO_LASTSAVED'], type='timestamp_ntz', alias='mro_lastsaved') }},
        {{ trx_json_extract('src', ['MRO_ORDEREDMATLCOSTDEFCUR'], type='float', alias='mro_orderedmatlcostdefcur') }},
        {{ trx_json_extract('src', ['MRO_VALUEONHANDDEFCUR'], type='float', alias='mro_valueonhanddefcur') }},
        {{ trx_json_extract('src', ['MRO_MATERIALCOSTDEFCUR'], type='float', alias='mro_materialcostdefcur') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5bvdwmroinvpartstobystore') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['etl_load_datetime']) }}

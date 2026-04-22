{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ELECMAPPING',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ELM_LASTSAVED'], type='timestamp_ntz', alias='elm_lastsaved') }},
        {{ trx_json_extract('src', ['ELM_ENTITY'], type='varchar', alias='elm_entity') }},
        {{ trx_json_extract('src', ['ELM_BASECOL'], type='varchar', alias='elm_basecol') }},
        {{ trx_json_extract('src', ['ELM_COLTYPE'], type='varchar', alias='elm_coltype') }},
        {{ trx_json_extract('src', ['ELM_CODE'], type='float', alias='elm_code') }},
        {{ trx_json_extract('src', ['ELM_ARCHCOL'], type='varchar', alias='elm_archcol') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5elecmapping') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['elm_lastsaved']) }}

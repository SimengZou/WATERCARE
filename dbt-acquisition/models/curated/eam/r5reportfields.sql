{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5REPORTFIELDS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['RFL_LASTSAVED'], type='timestamp_ntz', alias='rfl_lastsaved') }},
        {{ trx_json_extract('src', ['RFL_LINE'], type='float', alias='rfl_line') }},
        {{ trx_json_extract('src', ['RFL_BOTNUMBER'], type='float', alias='rfl_botnumber') }},
        {{ trx_json_extract('src', ['RFL_FIELD'], type='varchar', alias='rfl_field') }},
        {{ trx_json_extract('src', ['RFL_SHOWFIELD'], type='varchar', alias='rfl_showfield') }},
        {{ trx_json_extract('src', ['RFL_WIDTH'], type='float', alias='rfl_width') }},
        {{ trx_json_extract('src', ['RFL_UPDATECOUNT'], type='float', alias='rfl_updatecount') }},
        {{ trx_json_extract('src', ['RFL_UPDATED'], type='timestamp_ntz', alias='rfl_updated') }},
        {{ trx_json_extract('src', ['RFL_FUNCTION'], type='varchar', alias='rfl_function') }},
        {{ trx_json_extract('src', ['RFL_DATATYPE'], type='varchar', alias='rfl_datatype') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5reportfields') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rfl_function', 'rfl_line']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rfl_lastsaved']) }}

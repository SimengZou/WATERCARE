{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5FIELDFILTERTYPE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['FFT_LASTSAVED'], type='timestamp_ntz', alias='fft_lastsaved') }},
        {{ trx_json_extract('src', ['FFT_TYPE'], type='varchar', alias='fft_type') }},
        {{ trx_json_extract('src', ['FFT_DEFAULTSCREENTYPE'], type='varchar', alias='fft_defaultscreentype') }},
        {{ trx_json_extract('src', ['FFT_UPDATECOUNT'], type='float', alias='fft_updatecount') }},
        {{ trx_json_extract('src', ['FFT_FUNCTION'], type='varchar', alias='fft_function') }},
        {{ trx_json_extract('src', ['FFT_RTYPE'], type='varchar', alias='fft_rtype') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5fieldfiltertype') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['fft_function', 'fft_type', 'fft_rtype']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['fft_lastsaved']) }}

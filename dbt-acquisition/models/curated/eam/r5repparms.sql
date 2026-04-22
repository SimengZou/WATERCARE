{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5REPPARMS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PMT_LASTSAVED'], type='timestamp_ntz', alias='pmt_lastsaved') }},
        {{ trx_json_extract('src', ['PMT_PARAMETER'], type='varchar', alias='pmt_parameter') }},
        {{ trx_json_extract('src', ['PMT_RENTITY'], type='varchar', alias='pmt_rentity') }},
        {{ trx_json_extract('src', ['PMT_RTYPE'], type='varchar', alias='pmt_rtype') }},
        {{ trx_json_extract('src', ['PMT_DATATYPE'], type='varchar', alias='pmt_datatype') }},
        {{ trx_json_extract('src', ['PMT_LENGTH'], type='float', alias='pmt_length') }},
        {{ trx_json_extract('src', ['PMT_FORMAT'], type='varchar', alias='pmt_format') }},
        {{ trx_json_extract('src', ['PMT_DEFAULT'], type='varchar', alias='pmt_default') }},
        {{ trx_json_extract('src', ['PMT_FIXED'], type='varchar', alias='pmt_fixed') }},
        {{ trx_json_extract('src', ['PMT_MANDATORY'], type='varchar', alias='pmt_mandatory') }},
        {{ trx_json_extract('src', ['PMT_UPPER'], type='varchar', alias='pmt_upper') }},
        {{ trx_json_extract('src', ['PMT_OPTIONS'], type='float', alias='pmt_options') }},
        {{ trx_json_extract('src', ['PMT_REMEMBER'], type='varchar', alias='pmt_remember') }},
        {{ trx_json_extract('src', ['PMT_TEST'], type='varchar', alias='pmt_test') }},
        {{ trx_json_extract('src', ['PMT_QUERY'], type='varchar', alias='pmt_query') }},
        {{ trx_json_extract('src', ['PMT_LOVFUNCTION'], type='varchar', alias='pmt_lovfunction') }},
        {{ trx_json_extract('src', ['PMT_PROPERTY'], type='varchar', alias='pmt_property') }},
        {{ trx_json_extract('src', ['PMT_UPDATECOUNT'], type='float', alias='pmt_updatecount') }},
        {{ trx_json_extract('src', ['PMT_EWSLOVDEF'], type='varchar', alias='pmt_ewslovdef') }},
        {{ trx_json_extract('src', ['PMT_BOTTEXT'], type='varchar', alias='pmt_bottext') }},
        {{ trx_json_extract('src', ['PMT_UPDATED'], type='timestamp_ntz', alias='pmt_updated') }},
        {{ trx_json_extract('src', ['PMT_MEKEY'], type='varchar', alias='pmt_mekey') }},
        {{ trx_json_extract('src', ['PMT_FUNCTION'], type='varchar', alias='pmt_function') }},
        {{ trx_json_extract('src', ['PMT_LINE'], type='float', alias='pmt_line') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5repparms') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pmt_function', 'pmt_parameter']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pmt_lastsaved']) }}

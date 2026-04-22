{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5JOBPARAM',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['JPR_LASTSAVED'], type='timestamp_ntz', alias='jpr_lastsaved') }},
        {{ trx_json_extract('src', ['JPR_PARAMETER'], type='varchar', alias='jpr_parameter') }},
        {{ trx_json_extract('src', ['JPR_RENTITY'], type='varchar', alias='jpr_rentity') }},
        {{ trx_json_extract('src', ['JPR_RTYPE'], type='varchar', alias='jpr_rtype') }},
        {{ trx_json_extract('src', ['JPR_DATATYPE'], type='varchar', alias='jpr_datatype') }},
        {{ trx_json_extract('src', ['JPR_LENGTH'], type='float', alias='jpr_length') }},
        {{ trx_json_extract('src', ['JPR_FORMAT'], type='varchar', alias='jpr_format') }},
        {{ trx_json_extract('src', ['JPR_DEFAULT'], type='varchar', alias='jpr_default') }},
        {{ trx_json_extract('src', ['JPR_FIXED'], type='varchar', alias='jpr_fixed') }},
        {{ trx_json_extract('src', ['JPR_MANDATORY'], type='varchar', alias='jpr_mandatory') }},
        {{ trx_json_extract('src', ['JPR_UPPER'], type='varchar', alias='jpr_upper') }},
        {{ trx_json_extract('src', ['JPR_OPTIONS'], type='float', alias='jpr_options') }},
        {{ trx_json_extract('src', ['JPR_REMEMBER'], type='varchar', alias='jpr_remember') }},
        {{ trx_json_extract('src', ['JPR_TEST'], type='varchar', alias='jpr_test') }},
        {{ trx_json_extract('src', ['JPR_QUERY'], type='varchar', alias='jpr_query') }},
        {{ trx_json_extract('src', ['JPR_LOVFUNCTION'], type='varchar', alias='jpr_lovfunction') }},
        {{ trx_json_extract('src', ['JPR_PROPERTY'], type='varchar', alias='jpr_property') }},
        {{ trx_json_extract('src', ['JPR_EWSLOVDEF'], type='varchar', alias='jpr_ewslovdef') }},
        {{ trx_json_extract('src', ['JPR_UPDATECOUNT'], type='float', alias='jpr_updatecount') }},
        {{ trx_json_extract('src', ['JPR_NAME'], type='varchar', alias='jpr_name') }},
        {{ trx_json_extract('src', ['JPR_LINE'], type='float', alias='jpr_line') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5jobparam') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['jpr_lastsaved']) }}

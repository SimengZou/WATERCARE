{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MAILEVENTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MAE_LASTSAVED'], type='timestamp_ntz', alias='mae_lastsaved') }},
        {{ trx_json_extract('src', ['MAE_DATE'], type='timestamp_ntz', alias='mae_date') }},
        {{ trx_json_extract('src', ['MAE_SEND'], type='varchar', alias='mae_send') }},
        {{ trx_json_extract('src', ['MAE_RSTATUS'], type='varchar', alias='mae_rstatus') }},
        {{ trx_json_extract('src', ['MAE_ERROR'], type='varchar', alias='mae_error') }},
        {{ trx_json_extract('src', ['MAE_PARAM1'], type='varchar', alias='mae_param1') }},
        {{ trx_json_extract('src', ['MAE_PARAM2'], type='varchar', alias='mae_param2') }},
        {{ trx_json_extract('src', ['MAE_PARAM3'], type='varchar', alias='mae_param3') }},
        {{ trx_json_extract('src', ['MAE_PARAM4'], type='varchar', alias='mae_param4') }},
        {{ trx_json_extract('src', ['MAE_PARAM5'], type='varchar', alias='mae_param5') }},
        {{ trx_json_extract('src', ['MAE_PARAM6'], type='varchar', alias='mae_param6') }},
        {{ trx_json_extract('src', ['MAE_PARAM7'], type='varchar', alias='mae_param7') }},
        {{ trx_json_extract('src', ['MAE_PARAM8'], type='varchar', alias='mae_param8') }},
        {{ trx_json_extract('src', ['MAE_PARAM9'], type='varchar', alias='mae_param9') }},
        {{ trx_json_extract('src', ['MAE_PARAM10'], type='varchar', alias='mae_param10') }},
        {{ trx_json_extract('src', ['MAE_PARAM11'], type='varchar', alias='mae_param11') }},
        {{ trx_json_extract('src', ['MAE_PARAM12'], type='varchar', alias='mae_param12') }},
        {{ trx_json_extract('src', ['MAE_PARAM13'], type='varchar', alias='mae_param13') }},
        {{ trx_json_extract('src', ['MAE_PARAM14'], type='varchar', alias='mae_param14') }},
        {{ trx_json_extract('src', ['MAE_PARAM15'], type='varchar', alias='mae_param15') }},
        {{ trx_json_extract('src', ['MAE_UPDATECOUNT'], type='float', alias='mae_updatecount') }},
        {{ trx_json_extract('src', ['MAE_ATTRIBPK'], type='varchar', alias='mae_attribpk') }},
        {{ trx_json_extract('src', ['MAE_EWSURL'], type='varchar', alias='mae_ewsurl') }},
        {{ trx_json_extract('src', ['MAE_EMAILRECIPIENT'], type='varchar', alias='mae_emailrecipient') }},
        {{ trx_json_extract('src', ['MAE_PTFSEND'], type='varchar', alias='mae_ptfsend') }},
        {{ trx_json_extract('src', ['MAE_PTFERROR'], type='varchar', alias='mae_ptferror') }},
        {{ trx_json_extract('src', ['MAE_DOCRENTITY'], type='varchar', alias='mae_docrentity') }},
        {{ trx_json_extract('src', ['MAE_DOCPK'], type='varchar', alias='mae_docpk') }},
        {{ trx_json_extract('src', ['MAE_EMAILERRCOUNT'], type='float', alias='mae_emailerrcount') }},
        {{ trx_json_extract('src', ['MAE_PTFERRCOUNT'], type='float', alias='mae_ptferrcount') }},
        {{ trx_json_extract('src', ['MAE_CODE'], type='varchar', alias='mae_code') }},
        {{ trx_json_extract('src', ['MAE_TEMPLATE'], type='varchar', alias='mae_template') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mailevents') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['mae_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['mae_lastsaved']) }}

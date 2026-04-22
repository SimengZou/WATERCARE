{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ALERTGRIDPARAMS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['AGP_LASTSAVED'], type='timestamp_ntz', alias='agp_lastsaved') }},
        {{ trx_json_extract('src', ['AGP_PARAM'], type='varchar', alias='agp_param') }},
        {{ trx_json_extract('src', ['AGP_VALUE'], type='varchar', alias='agp_value') }},
        {{ trx_json_extract('src', ['AGP_DVALUE'], type='timestamp_ntz', alias='agp_dvalue') }},
        {{ trx_json_extract('src', ['AGP_UPDATECOUNT'], type='float', alias='agp_updatecount') }},
        {{ trx_json_extract('src', ['AGP_ALERT'], type='varchar', alias='agp_alert') }},
        {{ trx_json_extract('src', ['AGP_NVALUE'], type='float', alias='agp_nvalue') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5alertgridparams') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['agp_alert', 'agp_param']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['agp_lastsaved']) }}

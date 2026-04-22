{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PMPSESSIONDATES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PPD_LASTSAVED'], type='timestamp_ntz', alias='ppd_lastsaved') }},
        {{ trx_json_extract('src', ['PPD_PPSPK'], type='float', alias='ppd_ppspk') }},
        {{ trx_json_extract('src', ['PPD_DUEDATE'], type='timestamp_ntz', alias='ppd_duedate') }},
        {{ trx_json_extract('src', ['PPD_ISCALDATE'], type='varchar', alias='ppd_iscaldate') }},
        {{ trx_json_extract('src', ['PPD_ISPROJECTED'], type='varchar', alias='ppd_isprojected') }},
        {{ trx_json_extract('src', ['PPD_UPDATECOUNT'], type='float', alias='ppd_updatecount') }},
        {{ trx_json_extract('src', ['PPD_LINE'], type='float', alias='ppd_line') }},
        {{ trx_json_extract('src', ['PPD_WORKORDER'], type='varchar', alias='ppd_workorder') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5pmpsessiondates') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ppd_line', 'ppd_ppspk', 'ppd_duedate']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ppd_lastsaved']) }}

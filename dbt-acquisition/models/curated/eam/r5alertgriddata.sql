{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ALERTGRIDDATA',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['AGD_LASTSAVED'], type='timestamp_ntz', alias='agd_lastsaved') }},
        {{ trx_json_extract('src', ['AGD_ALERT'], type='varchar', alias='agd_alert') }},
        {{ trx_json_extract('src', ['AGD_GRIDID'], type='float', alias='agd_gridid') }},
        {{ trx_json_extract('src', ['AGD_DATASPYID'], type='float', alias='agd_dataspyid') }},
        {{ trx_json_extract('src', ['AGD_RECIPIENT'], type='varchar', alias='agd_recipient') }},
        {{ trx_json_extract('src', ['AGD_DESCRIPTION'], type='varchar', alias='agd_description') }},
        {{ trx_json_extract('src', ['AGD_DATA'], type='varchar', alias='agd_data') }},
        {{ trx_json_extract('src', ['AGD_PK'], type='varchar', alias='agd_pk') }},
        {{ trx_json_extract('src', ['AGD_DATE'], type='timestamp_ntz', alias='agd_date') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5alertgriddata') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['agd_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['agd_lastsaved']) }}

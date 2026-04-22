{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ORGYEARS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['OYE_LASTSAVED'], type='timestamp_ntz', alias='oye_lastsaved') }},
        {{ trx_json_extract('src', ['OYE_ORG'], type='varchar', alias='oye_org') }},
        {{ trx_json_extract('src', ['OYE_END'], type='timestamp_ntz', alias='oye_end') }},
        {{ trx_json_extract('src', ['OYE_UPDATECOUNT'], type='float', alias='oye_updatecount') }},
        {{ trx_json_extract('src', ['OYE_PK'], type='float', alias='oye_pk') }},
        {{ trx_json_extract('src', ['OYE_START'], type='timestamp_ntz', alias='oye_start') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5orgyears') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['oye_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['oye_lastsaved']) }}

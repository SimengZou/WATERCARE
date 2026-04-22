{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5UNAVAILABLEGRIDFIELDS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['UGF_LASTSAVED'], type='timestamp_ntz', alias='ugf_lastsaved') }},
        {{ trx_json_extract('src', ['UGF_GRIDNAME'], type='varchar', alias='ugf_gridname') }},
        {{ trx_json_extract('src', ['UGF_MEKEY'], type='varchar', alias='ugf_mekey') }},
        {{ trx_json_extract('src', ['UGF_USERGROUP'], type='varchar', alias='ugf_usergroup') }},
        {{ trx_json_extract('src', ['UGF_FIELDID'], type='varchar', alias='ugf_fieldid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5unavailablegridfields') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['ugf_gridname', 'ugf_usergroup', 'ugf_fieldid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['ugf_lastsaved']) }}

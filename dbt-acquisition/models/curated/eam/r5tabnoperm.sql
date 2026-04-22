{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5TABNOPERM',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['TPN_LASTSAVED'], type='timestamp_ntz', alias='tpn_lastsaved') }},
        {{ trx_json_extract('src', ['TPN_TAB'], type='varchar', alias='tpn_tab') }},
        {{ trx_json_extract('src', ['TPN_NOSELECT'], type='varchar', alias='tpn_noselect') }},
        {{ trx_json_extract('src', ['TPN_NODELETE'], type='varchar', alias='tpn_nodelete') }},
        {{ trx_json_extract('src', ['TPN_NOUPDATE'], type='varchar', alias='tpn_noupdate') }},
        {{ trx_json_extract('src', ['TPN_UPDATECOUNT'], type='float', alias='tpn_updatecount') }},
        {{ trx_json_extract('src', ['TPN_FUNCTION'], type='varchar', alias='tpn_function') }},
        {{ trx_json_extract('src', ['TPN_NOINSERT'], type='varchar', alias='tpn_noinsert') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5tabnoperm') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['tpn_function', 'tpn_tab']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['tpn_lastsaved']) }}

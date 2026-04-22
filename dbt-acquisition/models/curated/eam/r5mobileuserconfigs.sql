{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MOBILEUSERCONFIGS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MUC_LASTSAVED'], type='timestamp_ntz', alias='muc_lastsaved') }},
        {{ trx_json_extract('src', ['MUC_GROUP'], type='varchar', alias='muc_group') }},
        {{ trx_json_extract('src', ['MUC_CODE'], type='varchar', alias='muc_code') }},
        {{ trx_json_extract('src', ['MUC_DESK'], type='varchar', alias='muc_desk') }},
        {{ trx_json_extract('src', ['MUC_COMPTYPE'], type='varchar', alias='muc_comptype') }},
        {{ trx_json_extract('src', ['MUC_CREATED'], type='timestamp_ntz', alias='muc_created') }},
        {{ trx_json_extract('src', ['MUC_UPDATED'], type='timestamp_ntz', alias='muc_updated') }},
        {{ trx_json_extract('src', ['MUC_UPDATECOUNT'], type='float', alias='muc_updatecount') }},
        {{ trx_json_extract('src', ['MUC_USER'], type='varchar', alias='muc_user') }},
        {{ trx_json_extract('src', ['MUC_RCODE'], type='varchar', alias='muc_rcode') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mobileuserconfigs') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['muc_user', 'muc_group', 'muc_code', 'muc_desk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['muc_lastsaved']) }}

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5HOMEGROUPS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['HMG_LASTSAVED'], type='timestamp_ntz', alias='hmg_lastsaved') }},
        {{ trx_json_extract('src', ['HMG_HOMTYPE'], type='varchar', alias='hmg_homtype') }},
        {{ trx_json_extract('src', ['HMG_GROUP'], type='varchar', alias='hmg_group') }},
        {{ trx_json_extract('src', ['HMG_SEQ'], type='float', alias='hmg_seq') }},
        {{ trx_json_extract('src', ['HMG_AUTOFRESH'], type='varchar', alias='hmg_autofresh') }},
        {{ trx_json_extract('src', ['HMG_TAB'], type='varchar', alias='hmg_tab') }},
        {{ trx_json_extract('src', ['HMG_HOMCODE'], type='varchar', alias='hmg_homcode') }},
        {{ trx_json_extract('src', ['HMG_UPDATECOUNT'], type='float', alias='hmg_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5homegroups') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['hmg_homcode', 'hmg_homtype', 'hmg_group']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['hmg_lastsaved']) }}

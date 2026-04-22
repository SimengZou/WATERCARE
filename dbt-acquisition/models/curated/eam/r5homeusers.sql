{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5HOMEUSERS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['HMU_LASTSAVED'], type='timestamp_ntz', alias='hmu_lastsaved') }},
        {{ trx_json_extract('src', ['HMU_HOMTYPE'], type='varchar', alias='hmu_homtype') }},
        {{ trx_json_extract('src', ['HMU_USER'], type='varchar', alias='hmu_user') }},
        {{ trx_json_extract('src', ['HMU_AUTOFRESH'], type='varchar', alias='hmu_autofresh') }},
        {{ trx_json_extract('src', ['HMU_UPDATECOUNT'], type='float', alias='hmu_updatecount') }},
        {{ trx_json_extract('src', ['HMU_TAB'], type='varchar', alias='hmu_tab') }},
        {{ trx_json_extract('src', ['HMU_HOMCODE'], type='varchar', alias='hmu_homcode') }},
        {{ trx_json_extract('src', ['HMU_SEQ'], type='float', alias='hmu_seq') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5homeusers') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['hmu_homcode', 'hmu_homtype', 'hmu_user', 'hmu_seq']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['hmu_lastsaved']) }}

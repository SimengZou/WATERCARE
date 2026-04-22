{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5HIDDENDATASPIES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['HDS_LASTSAVED'], type='timestamp_ntz', alias='hds_lastsaved') }},
        {{ trx_json_extract('src', ['HDS_UPDATECOUNT'], type='float', alias='hds_updatecount') }},
        {{ trx_json_extract('src', ['HDS_GROUP'], type='varchar', alias='hds_group') }},
        {{ trx_json_extract('src', ['HDS_DATASPYID'], type='float', alias='hds_dataspyid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5hiddendataspies') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['hds_group', 'hds_dataspyid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['hds_lastsaved']) }}

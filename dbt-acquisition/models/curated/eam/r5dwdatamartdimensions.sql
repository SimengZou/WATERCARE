{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWDATAMARTDIMENSIONS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DMD_LASTSAVED'], type='timestamp_ntz', alias='dmd_lastsaved') }},
        {{ trx_json_extract('src', ['DMD_DIMTABLE'], type='varchar', alias='dmd_dimtable') }},
        {{ trx_json_extract('src', ['DMD_DIMBUSINESSKEYFIELD'], type='varchar', alias='dmd_dimbusinesskeyfield') }},
        {{ trx_json_extract('src', ['DMD_DMTTABLE'], type='varchar', alias='dmd_dmttable') }},
        {{ trx_json_extract('src', ['DMD_FACTJOINFIELD'], type='varchar', alias='dmd_factjoinfield') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwdatamartdimensions') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['dmd_lastsaved']) }}

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWDIMENSIONS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DIM_LASTSAVED'], type='timestamp_ntz', alias='dim_lastsaved') }},
        {{ trx_json_extract('src', ['DIM_DESC'], type='varchar', alias='dim_desc') }},
        {{ trx_json_extract('src', ['DIM_TABLETYPE'], type='varchar', alias='dim_tabletype') }},
        {{ trx_json_extract('src', ['DIM_CREATEKEYSEQUENCE'], type='varchar', alias='dim_createkeysequence') }},
        {{ trx_json_extract('src', ['DIM_SURROGATEKEYLOOKUPTBL'], type='varchar', alias='dim_surrogatekeylookuptbl') }},
        {{ trx_json_extract('src', ['DIM_BOTNUMBER'], type='float', alias='dim_botnumber') }},
        {{ trx_json_extract('src', ['DIM_TABLE'], type='varchar', alias='dim_table') }},
        {{ trx_json_extract('src', ['DIM_KEYFIELD'], type='varchar', alias='dim_keyfield') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwdimensions') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['dim_lastsaved']) }}

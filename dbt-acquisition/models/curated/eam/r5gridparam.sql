{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5GRIDPARAM',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['GDP_LASTSAVED'], type='timestamp_ntz', alias='gdp_lastsaved') }},
        {{ trx_json_extract('src', ['GDP_PARAM'], type='varchar', alias='gdp_param') }},
        {{ trx_json_extract('src', ['GDP_TAGNAME'], type='varchar', alias='gdp_tagname') }},
        {{ trx_json_extract('src', ['GDP_UPDATECOUNT'], type='float', alias='gdp_updatecount') }},
        {{ trx_json_extract('src', ['GDP_UPDATED'], type='timestamp_ntz', alias='gdp_updated') }},
        {{ trx_json_extract('src', ['GDP_GRIDID'], type='float', alias='gdp_gridid') }},
        {{ trx_json_extract('src', ['GDP_DATATYPE'], type='varchar', alias='gdp_datatype') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5gridparam') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['gdp_gridid', 'gdp_param']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['gdp_lastsaved']) }}

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PAGECACHE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PGC_LASTSAVED'], type='timestamp_ntz', alias='pgc_lastsaved') }},
        {{ trx_json_extract('src', ['PGC_FUNCTION'], type='varchar', alias='pgc_function') }},
        {{ trx_json_extract('src', ['PGC_TABNAME'], type='varchar', alias='pgc_tabname') }},
        {{ trx_json_extract('src', ['PGC_LAYOUTTYPE'], type='varchar', alias='pgc_layouttype') }},
        {{ trx_json_extract('src', ['PGC_JSPFILE'], type='varchar', alias='pgc_jspfile') }},
        {{ trx_json_extract('src', ['PGC_UPDATECOUNT'], type='float', alias='pgc_updatecount') }},
        {{ trx_json_extract('src', ['PGC_USERGROUP'], type='varchar', alias='pgc_usergroup') }},
        {{ trx_json_extract('src', ['PGC_RENTITY'], type='varchar', alias='pgc_rentity') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5pagecache') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['pgc_usergroup', 'pgc_function', 'pgc_tabname', 'pgc_layouttype']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['pgc_lastsaved']) }}

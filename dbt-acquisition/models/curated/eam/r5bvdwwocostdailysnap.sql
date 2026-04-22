{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5BVDWWOCOSTDAILYSNAP',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ZDT_KEY'], type='float', alias='zdt_key') }},
        {{ trx_json_extract('src', ['ZWO_KEY'], type='float', alias='zwo_key') }},
        {{ trx_json_extract('src', ['ZOR_KEY'], type='float', alias='zor_key') }},
        {{ trx_json_extract('src', ['ZMR_KEY'], type='float', alias='zmr_key') }},
        {{ trx_json_extract('src', ['ZOB_KEY'], type='float', alias='zob_key') }},
        {{ trx_json_extract('src', ['ZWT_ORGCUR'], type='varchar', alias='zwt_orgcur') }},
        {{ trx_json_extract('src', ['ZWT_DEFCUR'], type='varchar', alias='zwt_defcur') }},
        {{ trx_json_extract('src', ['ZWT_TOTALCOSTDEFCUR'], type='float', alias='zwt_totalcostdefcur') }},
        {{ trx_json_extract('src', ['ZWT_OWNLABCOSTDEFCUR'], type='float', alias='zwt_ownlabcostdefcur') }},
        {{ trx_json_extract('src', ['ZWT_OWNLABCOSTORGCUR'], type='float', alias='zwt_ownlabcostorgcur') }},
        {{ trx_json_extract('src', ['ZWT_HIREDLABCOSTDEFCUR'], type='float', alias='zwt_hiredlabcostdefcur') }},
        {{ trx_json_extract('src', ['ZWT_HIREDLABCOSTORGCUR'], type='float', alias='zwt_hiredlabcostorgcur') }},
        {{ trx_json_extract('src', ['ZWT_FIXEDLABCOSTDEFCUR'], type='float', alias='zwt_fixedlabcostdefcur') }},
        {{ trx_json_extract('src', ['ZWT_FIXEDLABCOSTORGCUR'], type='float', alias='zwt_fixedlabcostorgcur') }},
        {{ trx_json_extract('src', ['ZWT_STOCKMATCOSTDEFCUR'], type='float', alias='zwt_stockmatcostdefcur') }},
        {{ trx_json_extract('src', ['ZWT_STOCKMATCOSTORGCUR'], type='float', alias='zwt_stockmatcostorgcur') }},
        {{ trx_json_extract('src', ['ZWT_DIRMATCOSTDEFCUR'], type='float', alias='zwt_dirmatcostdefcur') }},
        {{ trx_json_extract('src', ['ZWT_DIRMATCOSTORGCUR'], type='float', alias='zwt_dirmatcostorgcur') }},
        {{ trx_json_extract('src', ['ZWT_TOOLCOSTDEFCUR'], type='float', alias='zwt_toolcostdefcur') }},
        {{ trx_json_extract('src', ['ZWT_TOOLCOSTORGCUR'], type='float', alias='zwt_toolcostorgcur') }},
        {{ trx_json_extract('src', ['ZWT_WOCODE'], type='varchar', alias='zwt_wocode') }},
        {{ trx_json_extract('src', ['ZWT_LASTSAVED'], type='timestamp_ntz', alias='zwt_lastsaved') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5bvdwwocostdailysnap') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['etl_load_datetime']) }}

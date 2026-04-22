{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5BVDWLABORDAILYSNAP',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ZDT_KEY'], type='float', alias='zdt_key') }},
        {{ trx_json_extract('src', ['ZWO_KEY'], type='float', alias='zwo_key') }},
        {{ trx_json_extract('src', ['ZPE_KEY'], type='float', alias='zpe_key') }},
        {{ trx_json_extract('src', ['ZTR_KEY'], type='float', alias='ztr_key') }},
        {{ trx_json_extract('src', ['ZMR_KEY'], type='float', alias='zmr_key') }},
        {{ trx_json_extract('src', ['ZOT_KEY'], type='float', alias='zot_key') }},
        {{ trx_json_extract('src', ['ZOR_KEY'], type='float', alias='zor_key') }},
        {{ trx_json_extract('src', ['ZLB_HOURS'], type='float', alias='zlb_hours') }},
        {{ trx_json_extract('src', ['ZLB_COSTDEFCUR'], type='float', alias='zlb_costdefcur') }},
        {{ trx_json_extract('src', ['ZLB_DEFCUR'], type='varchar', alias='zlb_defcur') }},
        {{ trx_json_extract('src', ['ZLB_COSTORGCUR'], type='float', alias='zlb_costorgcur') }},
        {{ trx_json_extract('src', ['ZLB_ORGCUR'], type='varchar', alias='zlb_orgcur') }},
        {{ trx_json_extract('src', ['ZLB_LASTSAVED'], type='timestamp_ntz', alias='zlb_lastsaved') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5bvdwlabordailysnap') }}
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

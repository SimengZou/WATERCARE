{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5DWDATES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ZDT_LASTSAVED'], type='timestamp_ntz', alias='zdt_lastsaved') }},
        {{ trx_json_extract('src', ['ZDT_DATE'], type='timestamp_ntz', alias='zdt_date') }},
        {{ trx_json_extract('src', ['ZDT_RUNNINGDAYNUM'], type='float', alias='zdt_runningdaynum') }},
        {{ trx_json_extract('src', ['ZDT_RUNNINGWEEKNUM'], type='float', alias='zdt_runningweeknum') }},
        {{ trx_json_extract('src', ['ZDT_RUNNINGMONTHNUM'], type='float', alias='zdt_runningmonthnum') }},
        {{ trx_json_extract('src', ['ZDT_DAYNUMINMONTH'], type='float', alias='zdt_daynuminmonth') }},
        {{ trx_json_extract('src', ['ZDT_DAYNUMINYEAR'], type='float', alias='zdt_daynuminyear') }},
        {{ trx_json_extract('src', ['ZDT_WEEKNUMINYEAR'], type='float', alias='zdt_weeknuminyear') }},
        {{ trx_json_extract('src', ['ZDT_MONTHNUMINYEAR'], type='float', alias='zdt_monthnuminyear') }},
        {{ trx_json_extract('src', ['ZDT_YEARMONTH'], type='varchar', alias='zdt_yearmonth') }},
        {{ trx_json_extract('src', ['ZDT_QUARTER'], type='float', alias='zdt_quarter') }},
        {{ trx_json_extract('src', ['ZDT_YEARQUARTER'], type='varchar', alias='zdt_yearquarter') }},
        {{ trx_json_extract('src', ['ZDT_HALFYEAR'], type='float', alias='zdt_halfyear') }},
        {{ trx_json_extract('src', ['ZDT_YEAR'], type='float', alias='zdt_year') }},
        {{ trx_json_extract('src', ['ZDT_LASTDAYINMONTH'], type='varchar', alias='zdt_lastdayinmonth') }},
        {{ trx_json_extract('src', ['ZDT_KEY'], type='float', alias='zdt_key') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5dwdates') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['zdt_lastsaved']) }}

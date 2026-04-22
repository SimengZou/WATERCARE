{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ALERTWOFIELDMAPPING',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['AWF_LASTSAVED'], type='timestamp_ntz', alias='awf_lastsaved') }},
        {{ trx_json_extract('src', ['AWF_WOFIELD'], type='varchar', alias='awf_wofield') }},
        {{ trx_json_extract('src', ['AWF_GRIDFIELD'], type='float', alias='awf_gridfield') }},
        {{ trx_json_extract('src', ['AWF_BOILERTEXTNUMBER'], type='float', alias='awf_boilertextnumber') }},
        {{ trx_json_extract('src', ['AWF_UPDATECOUNT'], type='float', alias='awf_updatecount') }},
        {{ trx_json_extract('src', ['AWF_ALERT'], type='varchar', alias='awf_alert') }},
        {{ trx_json_extract('src', ['AWF_GRIDFIELDDATATYPE'], type='varchar', alias='awf_gridfielddatatype') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5alertwofieldmapping') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['awf_alert', 'awf_wofield']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['awf_lastsaved']) }}

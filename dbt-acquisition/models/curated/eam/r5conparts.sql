{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CONPARTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CPA_LASTSAVED'], type='timestamp_ntz', alias='cpa_lastsaved') }},
        {{ trx_json_extract('src', ['CPA_SUPPLIER'], type='varchar', alias='cpa_supplier') }},
        {{ trx_json_extract('src', ['CPA_PART'], type='varchar', alias='cpa_part') }},
        {{ trx_json_extract('src', ['CPA_MULTIPLY'], type='float', alias='cpa_multiply') }},
        {{ trx_json_extract('src', ['CPA_LEADTIME'], type='float', alias='cpa_leadtime') }},
        {{ trx_json_extract('src', ['CPA_REF'], type='varchar', alias='cpa_ref') }},
        {{ trx_json_extract('src', ['CPA_PURUOM'], type='varchar', alias='cpa_puruom') }},
        {{ trx_json_extract('src', ['CPA_SUPPLIER_ORG'], type='varchar', alias='cpa_supplier_org') }},
        {{ trx_json_extract('src', ['CPA_PART_ORG'], type='varchar', alias='cpa_part_org') }},
        {{ trx_json_extract('src', ['CPA_UPDATECOUNT'], type='float', alias='cpa_updatecount') }},
        {{ trx_json_extract('src', ['CPA_CONTRACT'], type='varchar', alias='cpa_contract') }},
        {{ trx_json_extract('src', ['CPA_PRICE'], type='float', alias='cpa_price') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5conparts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cpa_contract', 'cpa_part', 'cpa_part_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['cpa_lastsaved']) }}

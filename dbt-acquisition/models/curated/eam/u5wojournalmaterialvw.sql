{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_U5WOJOURNALMATERIALVW',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['LASTSAVED'], type='timestamp_ntz', alias='lastsaved') }},
        {{ trx_json_extract('src', ['JRN_IDENTIFIER'], type='varchar', alias='jrn_identifier') }},
        {{ trx_json_extract('src', ['JRN_COSTTYPE'], type='varchar', alias='jrn_costtype') }},
        {{ trx_json_extract('src', ['JRN_TRNFLAG'], type='varchar', alias='jrn_trnflag') }},
        {{ trx_json_extract('src', ['JRN_COSTS'], type='float', alias='jrn_costs') }},
        {{ trx_json_extract('src', ['JRN_DEBCOSTCENTRE'], type='varchar', alias='jrn_debcostcentre') }},
        {{ trx_json_extract('src', ['JRN_DEBDIMENSION4'], type='varchar', alias='jrn_debdimension4') }},
        {{ trx_json_extract('src', ['JRN_CRECOSTCENTRE'], type='varchar', alias='jrn_crecostcentre') }},
        {{ trx_json_extract('src', ['JRN_CREDIMENSION3'], type='varchar', alias='jrn_credimension3') }},
        {{ trx_json_extract('src', ['JRN_CREDIMENSION4'], type='varchar', alias='jrn_credimension4') }},
        {{ trx_json_extract('src', ['JRN_PROJECTNUMBER'], type='varchar', alias='jrn_projectnumber') }},
        {{ trx_json_extract('src', ['JRN_PROJECTATIVITY'], type='varchar', alias='jrn_projectativity') }},
        {{ trx_json_extract('src', ['JRN_WORKORDER'], type='varchar', alias='jrn_workorder') }},
        {{ trx_json_extract('src', ['JRN_DEBDIMENSION3'], type='varchar', alias='jrn_debdimension3') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_u5wojournalmaterialvw') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate() }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['lastsaved']) }}

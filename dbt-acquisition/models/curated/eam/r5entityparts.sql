{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ENTITYPARTS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['EPA_LASTSAVED'], type='timestamp_ntz', alias='epa_lastsaved') }},
        {{ trx_json_extract('src', ['EPA_RENTITY'], type='varchar', alias='epa_rentity') }},
        {{ trx_json_extract('src', ['EPA_TYPE'], type='varchar', alias='epa_type') }},
        {{ trx_json_extract('src', ['EPA_RTYPE'], type='varchar', alias='epa_rtype') }},
        {{ trx_json_extract('src', ['EPA_CODE'], type='varchar', alias='epa_code') }},
        {{ trx_json_extract('src', ['EPA_QTY'], type='float', alias='epa_qty') }},
        {{ trx_json_extract('src', ['EPA_UOM'], type='varchar', alias='epa_uom') }},
        {{ trx_json_extract('src', ['EPA_COMMENT'], type='varchar', alias='epa_comment') }},
        {{ trx_json_extract('src', ['EPA_PART_ORG'], type='varchar', alias='epa_part_org') }},
        {{ trx_json_extract('src', ['EPA_UPDATECOUNT'], type='float', alias='epa_updatecount') }},
        {{ trx_json_extract('src', ['EPA_PK'], type='varchar', alias='epa_pk') }},
        {{ trx_json_extract('src', ['EPA_PARTLOCATION'], type='varchar', alias='epa_partlocation') }},
        {{ trx_json_extract('src', ['EPA_SYSLEVEL'], type='varchar', alias='epa_syslevel') }},
        {{ trx_json_extract('src', ['EPA_ASMLEVEL'], type='varchar', alias='epa_asmlevel') }},
        {{ trx_json_extract('src', ['EPA_COMPLEVEL'], type='varchar', alias='epa_complevel') }},
        {{ trx_json_extract('src', ['EPA_PART'], type='varchar', alias='epa_part') }},
        {{ trx_json_extract('src', ['EPA_ENTITY'], type='varchar', alias='epa_entity') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5entityparts') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['epa_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['epa_lastsaved']) }}

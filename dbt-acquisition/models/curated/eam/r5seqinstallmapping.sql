{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5SEQINSTALLMAPPING',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['SIM_LASTSAVED'], type='timestamp_ntz', alias='sim_lastsaved') }},
        {{ trx_json_extract('src', ['SIM_SEQUENCE'], type='varchar', alias='sim_sequence') }},
        {{ trx_json_extract('src', ['SIM_INSTALLPARAMETER'], type='varchar', alias='sim_installparameter') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5seqinstallmapping') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['sim_sequence']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sim_lastsaved']) }}

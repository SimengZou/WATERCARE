{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='dEPM to IDL for ProjectRisk for change request',
    tags=['auto-generated', 'depm', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['DocumentID'], type='varchar', alias='documentid') }},
        {{ trx_json_extract('src', ['DocumentDateTime'], type='timestamp_ntz', alias='documentdatetime') }},
        {{ trx_json_extract('src', ['Status'], type='varchar', alias='status') }},
        {{ trx_json_extract('src', ['Description'], type='varchar', alias='description') }},
        {{ trx_json_extract('src', ['RiskCategory'], type='varchar', alias='riskcategory') }},
        {{ trx_json_extract('src', ['RiskText'], type='varchar', alias='risktext') }},
        {{ trx_json_extract('src', ['TargetClosureDate'], type='timestamp_ntz', alias='targetclosuredate') }},
        {{ trx_json_extract('src', ['Project'], type='varchar', alias='project') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_depm', 'depm_r100_projectchange_testing') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['documentid']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['documentid']) }}

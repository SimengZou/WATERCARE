{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='Auto-generated model',
    tags=['auto-generated', 'depm', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ElementName'], type='varchar', alias='elementname') }},
        {{ trx_json_extract('src', ['Name'], type='varchar', alias='name') }},
        {{ trx_json_extract('src', ['LongName'], type='varchar', alias='longname') }},
        {{ trx_json_extract('src', ['Project'], type='varchar', alias='project') }},
        {{ trx_json_extract('src', ['Area'], type='varchar', alias='area') }},
        {{ trx_json_extract('src', ['SubArea'], type='varchar', alias='subarea') }},
        {{ trx_json_extract('src', ['UnitType'], type='varchar', alias='unittype') }},
        {{ trx_json_extract('src', ['TypeDesc'], type='varchar', alias='typedesc') }},
        {{ trx_json_extract('src', ['TypeCode'], type='varchar', alias='typecode') }},
        {{ trx_json_extract('src', ['Life'], type='varchar', alias='life') }},
        {{ trx_json_extract('src', ['ServiceStatus'], type='varchar', alias='servicestatus') }},
        {{ trx_json_extract('src', ['Model'], type='varchar', alias='model') }},
        {{ trx_json_extract('src', ['Serial'], type='varchar', alias='serial') }},
        {{ trx_json_extract('src', ['InstallDate'], type='varchar', alias='installdate') }},
        {{ trx_json_extract('src', ['AsBuilt'], type='varchar', alias='asbuilt') }},
        {{ trx_json_extract('src', ['CostCentre'], type='varchar', alias='costcentre') }},
        {{ trx_json_extract('src', ['AcqType'], type='varchar', alias='acqtype') }},
        {{ trx_json_extract('src', ['Timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_depm', 'depm_dim_asset') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['elementname']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['timestamp']) }}

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5USERDEFINEDFIELDSETUP',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['UDF_LASTSAVED'], type='timestamp_ntz', alias='udf_lastsaved') }},
        {{ trx_json_extract('src', ['UDF_MIN'], type='varchar', alias='udf_min') }},
        {{ trx_json_extract('src', ['UDF_MAX'], type='varchar', alias='udf_max') }},
        {{ trx_json_extract('src', ['UDF_PRINT'], type='varchar', alias='udf_print') }},
        {{ trx_json_extract('src', ['UDF_UOM'], type='varchar', alias='udf_uom') }},
        {{ trx_json_extract('src', ['UDF_LOOKUPTYPE'], type='varchar', alias='udf_lookuptype') }},
        {{ trx_json_extract('src', ['UDF_LOOKUPVALIDATE'], type='varchar', alias='udf_lookupvalidate') }},
        {{ trx_json_extract('src', ['UDF_LOOKUPRENTITY'], type='varchar', alias='udf_lookuprentity') }},
        {{ trx_json_extract('src', ['UDF_DATETYPE'], type='varchar', alias='udf_datetype') }},
        {{ trx_json_extract('src', ['UDF_NUMBERTYPE'], type='varchar', alias='udf_numbertype') }},
        {{ trx_json_extract('src', ['UDF_CURR'], type='varchar', alias='udf_curr') }},
        {{ trx_json_extract('src', ['UDF_ENABLEFORADDONS'], type='varchar', alias='udf_enableforaddons') }},
        {{ trx_json_extract('src', ['UDF_UPDATED'], type='timestamp_ntz', alias='udf_updated') }},
        {{ trx_json_extract('src', ['UDF_UPDATECOUNT'], type='float', alias='udf_updatecount') }},
        {{ trx_json_extract('src', ['UDF_RENTITY'], type='varchar', alias='udf_rentity') }},
        {{ trx_json_extract('src', ['UDF_FIELD'], type='varchar', alias='udf_field') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5userdefinedfieldsetup') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['udf_rentity', 'udf_field']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['udf_lastsaved']) }}

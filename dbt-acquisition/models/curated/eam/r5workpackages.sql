{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5WORKPACKAGES',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['WPK_LASTSAVED'], type='timestamp_ntz', alias='wpk_lastsaved') }},
        {{ trx_json_extract('src', ['WPK_ORG'], type='varchar', alias='wpk_org') }},
        {{ trx_json_extract('src', ['WPK_JOBTYPE'], type='varchar', alias='wpk_jobtype') }},
        {{ trx_json_extract('src', ['WPK_STATUS'], type='varchar', alias='wpk_status') }},
        {{ trx_json_extract('src', ['WPK_MRC'], type='varchar', alias='wpk_mrc') }},
        {{ trx_json_extract('src', ['WPK_CLASS'], type='varchar', alias='wpk_class') }},
        {{ trx_json_extract('src', ['WPK_CLASS_ORG'], type='varchar', alias='wpk_class_org') }},
        {{ trx_json_extract('src', ['WPK_JOBCLASS'], type='varchar', alias='wpk_jobclass') }},
        {{ trx_json_extract('src', ['WPK_JOBCLASS_ORG'], type='varchar', alias='wpk_jobclass_org') }},
        {{ trx_json_extract('src', ['WPK_PPMTYPE'], type='varchar', alias='wpk_ppmtype') }},
        {{ trx_json_extract('src', ['WPK_TRADE'], type='varchar', alias='wpk_trade') }},
        {{ trx_json_extract('src', ['WPK_OBJECT'], type='varchar', alias='wpk_object') }},
        {{ trx_json_extract('src', ['WPK_OBJECT_ORG'], type='varchar', alias='wpk_object_org') }},
        {{ trx_json_extract('src', ['WPK_LASTEVENT'], type='varchar', alias='wpk_lastevent') }},
        {{ trx_json_extract('src', ['WPK_DUEDATE'], type='timestamp_ntz', alias='wpk_duedate') }},
        {{ trx_json_extract('src', ['WPK_FREQ'], type='float', alias='wpk_freq') }},
        {{ trx_json_extract('src', ['WPK_DURATION'], type='float', alias='wpk_duration') }},
        {{ trx_json_extract('src', ['WPK_ESTWORKLOAD'], type='float', alias='wpk_estworkload') }},
        {{ trx_json_extract('src', ['WPK_PERSONS'], type='float', alias='wpk_persons') }},
        {{ trx_json_extract('src', ['WPK_CHANGED'], type='varchar', alias='wpk_changed') }},
        {{ trx_json_extract('src', ['WPK_PERIODUOM'], type='varchar', alias='wpk_perioduom') }},
        {{ trx_json_extract('src', ['WPK_UPDATECOUNT'], type='float', alias='wpk_updatecount') }},
        {{ trx_json_extract('src', ['WPK_PERFORMONWEEK'], type='varchar', alias='wpk_performonweek') }},
        {{ trx_json_extract('src', ['WPK_PERFORMONDAY'], type='float', alias='wpk_performonday') }},
        {{ trx_json_extract('src', ['WPK_CODE'], type='varchar', alias='wpk_code') }},
        {{ trx_json_extract('src', ['WPK_DESC'], type='varchar', alias='wpk_desc') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5workpackages') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['wpk_code']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['wpk_lastsaved']) }}

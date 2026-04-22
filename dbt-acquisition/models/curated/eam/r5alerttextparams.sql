{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5ALERTTEXTPARAMS',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['ATP_LASTSAVED'], type='timestamp_ntz', alias='atp_lastsaved') }},
        {{ trx_json_extract('src', ['ATP_RTYPE'], type='varchar', alias='atp_rtype') }},
        {{ trx_json_extract('src', ['ATP_TEMPLATE'], type='varchar', alias='atp_template') }},
        {{ trx_json_extract('src', ['ATP_PARAMETER'], type='float', alias='atp_parameter') }},
        {{ trx_json_extract('src', ['ATP_VALUE'], type='varchar', alias='atp_value') }},
        {{ trx_json_extract('src', ['ATP_INCLUDERECIPIENT'], type='varchar', alias='atp_includerecipient') }},
        {{ trx_json_extract('src', ['ATP_UPDATECOUNT'], type='float', alias='atp_updatecount') }},
        {{ trx_json_extract('src', ['ATP_REPORTPARAMETER'], type='float', alias='atp_reportparameter') }},
        {{ trx_json_extract('src', ['ATP_ALERT'], type='varchar', alias='atp_alert') }},
        {{ trx_json_extract('src', ['ATP_FIELDID'], type='float', alias='atp_fieldid') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5alerttextparams') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['atp_alert', 'atp_rtype', 'atp_template', 'atp_parameter']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['atp_lastsaved']) }}

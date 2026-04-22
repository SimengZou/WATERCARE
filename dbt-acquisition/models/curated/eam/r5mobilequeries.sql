{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MOBILEQUERIES',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MQR_LASTSAVED'], type='timestamp_ntz', alias='mqr_lastsaved') }},
        {{ trx_json_extract('src', ['MQR_VERSION'], type='varchar', alias='mqr_version') }},
        {{ trx_json_extract('src', ['MQR_TABLENAME'], type='varchar', alias='mqr_tablename') }},
        {{ trx_json_extract('src', ['MQR_CREATETABLE'], type='varchar', alias='mqr_createtable') }},
        {{ trx_json_extract('src', ['MQR_SQLTEXT'], type='varchar', alias='mqr_sqltext') }},
        {{ trx_json_extract('src', ['MQR_PREPAREGRID'], type='varchar', alias='mqr_preparegrid') }},
        {{ trx_json_extract('src', ['MQR_COLUMNMAP'], type='varchar', alias='mqr_columnmap') }},
        {{ trx_json_extract('src', ['MQR_SINGLETHREADPERCONN'], type='varchar', alias='mqr_singlethreadperconn') }},
        {{ trx_json_extract('src', ['MQR_PREPARETABLEUSED'], type='varchar', alias='mqr_preparetableused') }},
        {{ trx_json_extract('src', ['MQR_UPDATECOUNT'], type='float', alias='mqr_updatecount') }},
        {{ trx_json_extract('src', ['MQR_GRIDNAME'], type='varchar', alias='mqr_gridname') }},
        {{ trx_json_extract('src', ['MQR_UPDATED'], type='timestamp_ntz', alias='mqr_updated') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mobilequeries') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['mqr_gridname', 'mqr_tablename', 'mqr_version']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['mqr_lastsaved']) }}

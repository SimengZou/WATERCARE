{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5MOBILESYNCDELETE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['MSD_LASTSAVED'], type='timestamp_ntz', alias='msd_lastsaved') }},
        {{ trx_json_extract('src', ['MSD_TABLENAME'], type='varchar', alias='msd_tablename') }},
        {{ trx_json_extract('src', ['MSD_DELETED'], type='timestamp_ntz', alias='msd_deleted') }},
        {{ trx_json_extract('src', ['MSD_RENTITY'], type='varchar', alias='msd_rentity') }},
        {{ trx_json_extract('src', ['MSD_ORG'], type='varchar', alias='msd_org') }},
        {{ trx_json_extract('src', ['MSD_VALUES'], type='varchar', alias='msd_values') }},
        {{ trx_json_extract('src', ['MSD_UPDATECOUNT'], type='float', alias='msd_updatecount') }},
        {{ trx_json_extract('src', ['MSD_PK'], type='float', alias='msd_pk') }},
        {{ trx_json_extract('src', ['MSD_KEYS'], type='varchar', alias='msd_keys') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5mobilesyncdelete') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['msd_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['msd_lastsaved']) }}

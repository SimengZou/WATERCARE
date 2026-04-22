{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5RELIABILITYRANKINGRANKS',
    tags=['auto-generated', 'eam', 'stage2']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['RRR_LASTSAVED'], type='timestamp_ntz', alias='rrr_lastsaved') }},
        {{ trx_json_extract('src', ['RRR_RELIABILITYRANKING'], type='varchar', alias='rrr_reliabilityranking') }},
        {{ trx_json_extract('src', ['RRR_ORG'], type='varchar', alias='rrr_org') }},
        {{ trx_json_extract('src', ['RRR_REVISION'], type='float', alias='rrr_revision') }},
        {{ trx_json_extract('src', ['RRR_MINVALUE'], type='float', alias='rrr_minvalue') }},
        {{ trx_json_extract('src', ['RRR_MAXVALUE'], type='float', alias='rrr_maxvalue') }},
        {{ trx_json_extract('src', ['RRR_IMAGE'], type='varchar', alias='rrr_image') }},
        {{ trx_json_extract('src', ['RRR_UPDATECOUNT'], type='float', alias='rrr_updatecount') }},
        {{ trx_json_extract('src', ['RRR_CRITICALITY'], type='varchar', alias='rrr_criticality') }},
        {{ trx_json_extract('src', ['RRR_COLOR'], type='varchar', alias='rrr_color') }},
        {{ trx_json_extract('src', ['RRR_ICON'], type='varchar', alias='rrr_icon') }},
        {{ trx_json_extract('src', ['RRR_ICONPATH'], type='varchar', alias='rrr_iconpath') }},
        {{ trx_json_extract('src', ['RRR_PK'], type='varchar', alias='rrr_pk') }},
        {{ trx_json_extract('src', ['RRR_RRINDEX'], type='varchar', alias='rrr_rrindex') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5reliabilityrankingranks') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['rrr_pk']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['rrr_lastsaved']) }}

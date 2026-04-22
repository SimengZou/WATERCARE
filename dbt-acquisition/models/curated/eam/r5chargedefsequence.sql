{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CHARGEDEFSEQUENCE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CDS_LASTSAVED'], type='timestamp_ntz', alias='cds_lastsaved') }},
        {{ trx_json_extract('src', ['CDS_SUBCATEGORY'], type='varchar', alias='cds_subcategory') }},
        {{ trx_json_extract('src', ['CDS_LEVEL'], type='varchar', alias='cds_level') }},
        {{ trx_json_extract('src', ['CDS_SEQUENCE'], type='float', alias='cds_sequence') }},
        {{ trx_json_extract('src', ['CDS_UPDATECOUNT'], type='float', alias='cds_updatecount') }},
        {{ trx_json_extract('src', ['CDS_CATEGORY'], type='varchar', alias='cds_category') }},
        {{ trx_json_extract('src', ['CDS_ACTUALSUBCAT'], type='varchar', alias='cds_actualsubcat') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5chargedefsequence') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cds_category', 'cds_subcategory', 'cds_level', 'cds_sequence']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['cds_lastsaved']) }}

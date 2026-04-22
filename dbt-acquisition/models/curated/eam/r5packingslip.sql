{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5PACKINGSLIP',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['PSL_LASTSAVED'], type='timestamp_ntz', alias='psl_lastsaved') }},
        {{ trx_json_extract('src', ['PSL_BIN'], type='varchar', alias='psl_bin') }},
        {{ trx_json_extract('src', ['PSL_ORDER'], type='varchar', alias='psl_order') }},
        {{ trx_json_extract('src', ['PSL_ORDLINE'], type='float', alias='psl_ordline') }},
        {{ trx_json_extract('src', ['PSL_ORDER_ORG'], type='varchar', alias='psl_order_org') }},
        {{ trx_json_extract('src', ['PSL_PART'], type='varchar', alias='psl_part') }},
        {{ trx_json_extract('src', ['PSL_PART_ORG'], type='varchar', alias='psl_part_org') }},
        {{ trx_json_extract('src', ['PSL_MANLOT'], type='varchar', alias='psl_manlot') }},
        {{ trx_json_extract('src', ['PSL_MANLOTEXP'], type='timestamp_ntz', alias='psl_manlotexp') }},
        {{ trx_json_extract('src', ['PSL_SUPPLIERREF'], type='varchar', alias='psl_supplierref') }},
        {{ trx_json_extract('src', ['PSL_DELUOM'], type='varchar', alias='psl_deluom') }},
        {{ trx_json_extract('src', ['PSL_DELQTY'], type='float', alias='psl_delqty') }},
        {{ trx_json_extract('src', ['PSL_RECVQTY'], type='float', alias='psl_recvqty') }},
        {{ trx_json_extract('src', ['PSL_MULTIPLY'], type='float', alias='psl_multiply') }},
        {{ trx_json_extract('src', ['PSL_COMMENT'], type='varchar', alias='psl_comment') }},
        {{ trx_json_extract('src', ['PSL_UPDATECOUNT'], type='float', alias='psl_updatecount') }},
        {{ trx_json_extract('src', ['PSL_DOCK'], type='varchar', alias='psl_dock') }},
        {{ trx_json_extract('src', ['PSL_LINE'], type='float', alias='psl_line') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5packingslip') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['psl_dock', 'psl_line']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['psl_lastsaved']) }}

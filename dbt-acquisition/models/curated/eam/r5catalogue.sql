{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='eam_R5CATALOGUE',
    tags=['auto-generated', 'eam', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['CAT_LASTSAVED'], type='timestamp_ntz', alias='cat_lastsaved') }},
        {{ trx_json_extract('src', ['CAT_REPREF'], type='varchar', alias='cat_repref') }},
        {{ trx_json_extract('src', ['CAT_IPLASTUPDATE'], type='timestamp_ntz', alias='cat_iplastupdate') }},
        {{ trx_json_extract('src', ['CAT_IPERROR'], type='float', alias='cat_iperror') }},
        {{ trx_json_extract('src', ['CAT_IPERRORMESSAGE'], type='varchar', alias='cat_iperrormessage') }},
        {{ trx_json_extract('src', ['CAT_INSERTOLDREFERENCE'], type='varchar', alias='cat_insertoldreference') }},
        {{ trx_json_extract('src', ['CAT_COSTCODE'], type='varchar', alias='cat_costcode') }},
        {{ trx_json_extract('src', ['CAT_DOCUMOTO_BOOKID'], type='varchar', alias='cat_documoto_bookid') }},
        {{ trx_json_extract('src', ['CAT_DOCUMOTO_PART'], type='varchar', alias='cat_documoto_part') }},
        {{ trx_json_extract('src', ['CAT_PART'], type='varchar', alias='cat_part') }},
        {{ trx_json_extract('src', ['CAT_SUPPLIER'], type='varchar', alias='cat_supplier') }},
        {{ trx_json_extract('src', ['CAT_DATE'], type='timestamp_ntz', alias='cat_date') }},
        {{ trx_json_extract('src', ['CAT_GROSS'], type='float', alias='cat_gross') }},
        {{ trx_json_extract('src', ['CAT_LEADTIME'], type='float', alias='cat_leadtime') }},
        {{ trx_json_extract('src', ['CAT_QUOTATION'], type='varchar', alias='cat_quotation') }},
        {{ trx_json_extract('src', ['CAT_EXPQUOT'], type='timestamp_ntz', alias='cat_expquot') }},
        {{ trx_json_extract('src', ['CAT_QUOTPRICE'], type='float', alias='cat_quotprice') }},
        {{ trx_json_extract('src', ['CAT_QUOTUOM'], type='varchar', alias='cat_quotuom') }},
        {{ trx_json_extract('src', ['CAT_EXPPURPR'], type='timestamp_ntz', alias='cat_exppurpr') }},
        {{ trx_json_extract('src', ['CAT_PURPRICE'], type='float', alias='cat_purprice') }},
        {{ trx_json_extract('src', ['CAT_PURUOM'], type='varchar', alias='cat_puruom') }},
        {{ trx_json_extract('src', ['CAT_REF'], type='varchar', alias='cat_ref') }},
        {{ trx_json_extract('src', ['CAT_MULTIPLY'], type='float', alias='cat_multiply') }},
        {{ trx_json_extract('src', ['CAT_CURR'], type='varchar', alias='cat_curr') }},
        {{ trx_json_extract('src', ['CAT_EXCH'], type='float', alias='cat_exch') }},
        {{ trx_json_extract('src', ['CAT_TAX'], type='varchar', alias='cat_tax') }},
        {{ trx_json_extract('src', ['CAT_SOURCESYSTEM'], type='varchar', alias='cat_sourcesystem') }},
        {{ trx_json_extract('src', ['CAT_SOURCECODE'], type='varchar', alias='cat_sourcecode') }},
        {{ trx_json_extract('src', ['CAT_EXCHTODUAL'], type='float', alias='cat_exchtodual') }},
        {{ trx_json_extract('src', ['CAT_EXCHFROMDUAL'], type='float', alias='cat_exchfromdual') }},
        {{ trx_json_extract('src', ['CAT_SUPPLIER_ORG'], type='varchar', alias='cat_supplier_org') }},
        {{ trx_json_extract('src', ['CAT_PART_ORG'], type='varchar', alias='cat_part_org') }},
        {{ trx_json_extract('src', ['CAT_DESC'], type='varchar', alias='cat_desc') }},
        {{ trx_json_extract('src', ['CAT_MINORDQTY'], type='float', alias='cat_minordqty') }},
        {{ trx_json_extract('src', ['CAT_REPPRICE'], type='float', alias='cat_repprice') }},
        {{ trx_json_extract('src', ['CAT_UPDATECOUNT'], type='float', alias='cat_updatecount') }},
        {{ trx_json_extract('src', ['_DELETED'], type='boolean', alias='_deleted') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_eam', 'eam_r5catalogue') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['cat_part', 'cat_part_org', 'cat_supplier', 'cat_supplier_org']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['cat_lastsaved']) }}

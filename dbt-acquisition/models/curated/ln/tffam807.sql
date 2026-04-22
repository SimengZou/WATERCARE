{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN tffam807 Transaction Summary table, Generated 2026-04-10T19:41:35Z from package combination ce01101',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['comp'], type='int', alias='comp') }},
        {{ trx_json_extract('src', ['acct'], type='varchar', alias='acct') }},
        {{ trx_json_extract('src', ['dim1'], type='varchar', alias='dim1') }},
        {{ trx_json_extract('src', ['dim2'], type='varchar', alias='dim2') }},
        {{ trx_json_extract('src', ['dim3'], type='varchar', alias='dim3') }},
        {{ trx_json_extract('src', ['dim4'], type='varchar', alias='dim4') }},
        {{ trx_json_extract('src', ['dim5'], type='varchar', alias='dim5') }},
        {{ trx_json_extract('src', ['dim6'], type='varchar', alias='dim6') }},
        {{ trx_json_extract('src', ['dim7'], type='varchar', alias='dim7') }},
        {{ trx_json_extract('src', ['dim8'], type='varchar', alias='dim8') }},
        {{ trx_json_extract('src', ['dim9'], type='varchar', alias='dim9') }},
        {{ trx_json_extract('src', ['dm10'], type='varchar', alias='dm10') }},
        {{ trx_json_extract('src', ['dm11'], type='varchar', alias='dm11') }},
        {{ trx_json_extract('src', ['dm12'], type='varchar', alias='dm12') }},
        {{ trx_json_extract('src', ['dims'], type='varchar', alias='dims') }},
        {{ trx_json_extract('src', ['book'], type='varchar', alias='book') }},
        {{ trx_json_extract('src', ['lkey'], type='int', alias='lkey') }},
        {{ trx_json_extract('src', ['idtc'], type='varchar', alias='idtc') }},
        {{ trx_json_extract('src', ['year'], type='int', alias='year') }},
        {{ trx_json_extract('src', ['prod'], type='int', alias='prod') }},
        {{ trx_json_extract('src', ['acqa_1'], type='float', alias='acqa_1') }},
        {{ trx_json_extract('src', ['acqa_2'], type='float', alias='acqa_2') }},
        {{ trx_json_extract('src', ['acqa_3'], type='float', alias='acqa_3') }},
        {{ trx_json_extract('src', ['adja_1'], type='float', alias='adja_1') }},
        {{ trx_json_extract('src', ['adja_2'], type='float', alias='adja_2') }},
        {{ trx_json_extract('src', ['adja_3'], type='float', alias='adja_3') }},
        {{ trx_json_extract('src', ['depr_1'], type='float', alias='depr_1') }},
        {{ trx_json_extract('src', ['depr_2'], type='float', alias='depr_2') }},
        {{ trx_json_extract('src', ['depr_3'], type='float', alias='depr_3') }},
        {{ trx_json_extract('src', ['disa_1'], type='float', alias='disa_1') }},
        {{ trx_json_extract('src', ['disa_2'], type='float', alias='disa_2') }},
        {{ trx_json_extract('src', ['disa_3'], type='float', alias='disa_3') }},
        {{ trx_json_extract('src', ['proa_1'], type='float', alias='proa_1') }},
        {{ trx_json_extract('src', ['proa_2'], type='float', alias='proa_2') }},
        {{ trx_json_extract('src', ['proa_3'], type='float', alias='proa_3') }},
        {{ trx_json_extract('src', ['traa_1'], type='float', alias='traa_1') }},
        {{ trx_json_extract('src', ['traa_2'], type='float', alias='traa_2') }},
        {{ trx_json_extract('src', ['traa_3'], type='float', alias='traa_3') }},
        {{ trx_json_extract('src', ['rcst_1'], type='float', alias='rcst_1') }},
        {{ trx_json_extract('src', ['rcst_2'], type='float', alias='rcst_2') }},
        {{ trx_json_extract('src', ['rcst_3'], type='float', alias='rcst_3') }},
        {{ trx_json_extract('src', ['rdpr_1'], type='float', alias='rdpr_1') }},
        {{ trx_json_extract('src', ['rdpr_2'], type='float', alias='rdpr_2') }},
        {{ trx_json_extract('src', ['rdpr_3'], type='float', alias='rdpr_3') }},
        {{ trx_json_extract('src', ['trct_1'], type='float', alias='trct_1') }},
        {{ trx_json_extract('src', ['trct_2'], type='float', alias='trct_2') }},
        {{ trx_json_extract('src', ['trct_3'], type='float', alias='trct_3') }},
        {{ trx_json_extract('src', ['book_ref_compnr'], type='int', alias='book_ref_compnr') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_tffam807') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'comp', 'acct', 'dim1', 'dim2', 'dim3', 'dim4', 'dim5', 'dims', 'book', 'lkey', 'idtc', 'year', 'prod']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}

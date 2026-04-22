{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='dbt_surrogate_key',
    description='LN qmptc115 Inspection Order Test Data table, Generated 2022-06-15T01:21:06Z from package combination ce01055',
    tags=['auto-generated', 'ln', 'stage1']
) }}

with cte_flattened as (
    select
        {{ trx_json_extract('src', ['compnr'], type='int', alias='compnr') }},
        {{ trx_json_extract('src', ['iorn'], type='varchar', alias='iorn') }},
        {{ trx_json_extract('src', ['saml'], type='int', alias='saml') }},
        {{ trx_json_extract('src', ['pono'], type='int', alias='pono') }},
        {{ trx_json_extract('src', ['srno'], type='int', alias='srno') }},
        {{ trx_json_extract('src', ['tseq'], type='int', alias='tseq') }},
        {{ trx_json_extract('src', ['tsdt'], type='timestamp_ntz', alias='tsdt') }},
        {{ trx_json_extract('src', ['mval'], type='float', alias='mval') }},
        {{ trx_json_extract('src', ['oset'], type='varchar', alias='oset') }},
        {{ trx_json_extract('src', ['mopt'], type='varchar', alias='mopt') }},
        {{ trx_json_extract('src', ['tare'], type='varchar', alias='tare') }},
        {{ trx_json_extract('src', ['emno'], type='varchar', alias='emno') }},
        {{ trx_json_extract('src', ['inst'], type='varchar', alias='inst') }},
        {{ trx_json_extract('src', ['inno'], type='varchar', alias='inno') }},
        {{ trx_json_extract('src', ['tqua'], type='float', alias='tqua') }},
        {{ trx_json_extract('src', ['tuni'], type='varchar', alias='tuni') }},
        {{ trx_json_extract('src', ['resl'], type='int', alias='resl') }},
        {{ trx_json_extract('src', ['resl_kw'], type='varchar', alias='resl_kw') }},
        {{ trx_json_extract('src', ['ncmr'], type='varchar', alias='ncmr') }},
        {{ trx_json_extract('src', ['aemn'], type='varchar', alias='aemn') }},
        {{ trx_json_extract('src', ['etim'], type='float', alias='etim') }},
        {{ trx_json_extract('src', ['aemp'], type='varchar', alias='aemp') }},
        {{ trx_json_extract('src', ['text'], type='int', alias='text') }},
        {{ trx_json_extract('src', ['iorn_saml_ref_compnr'], type='int', alias='iorn_saml_ref_compnr') }},
        {{ trx_json_extract('src', ['iorn_pono_ref_compnr'], type='int', alias='iorn_pono_ref_compnr') }},
        {{ trx_json_extract('src', ['iorn_ref_compnr'], type='int', alias='iorn_ref_compnr') }},
        {{ trx_json_extract('src', ['oset_mopt_ref_compnr'], type='int', alias='oset_mopt_ref_compnr') }},
        {{ trx_json_extract('src', ['tare_ref_compnr'], type='int', alias='tare_ref_compnr') }},
        {{ trx_json_extract('src', ['emno_ref_compnr'], type='int', alias='emno_ref_compnr') }},
        {{ trx_json_extract('src', ['inst_inno_ref_compnr'], type='int', alias='inst_inno_ref_compnr') }},
        {{ trx_json_extract('src', ['tuni_ref_compnr'], type='int', alias='tuni_ref_compnr') }},
        {{ trx_json_extract('src', ['ncmr_ref_compnr'], type='int', alias='ncmr_ref_compnr') }},
        {{ trx_json_extract('src', ['aemn_ref_compnr'], type='int', alias='aemn_ref_compnr') }},
        {{ trx_json_extract('src', ['text_ref_compnr'], type='int', alias='text_ref_compnr') }},
        {{ trx_json_extract('src', ['mval_buar'], type='float', alias='mval_buar') }},
        {{ trx_json_extract('src', ['mval_buln'], type='float', alias='mval_buln') }},
        {{ trx_json_extract('src', ['mval_bupc'], type='float', alias='mval_bupc') }},
        {{ trx_json_extract('src', ['mval_butm'], type='float', alias='mval_butm') }},
        {{ trx_json_extract('src', ['mval_buvl'], type='float', alias='mval_buvl') }},
        {{ trx_json_extract('src', ['mval_buwg'], type='float', alias='mval_buwg') }},
        {{ trx_json_extract('src', ['tqua_buar'], type='float', alias='tqua_buar') }},
        {{ trx_json_extract('src', ['tqua_buln'], type='float', alias='tqua_buln') }},
        {{ trx_json_extract('src', ['tqua_bupc'], type='float', alias='tqua_bupc') }},
        {{ trx_json_extract('src', ['tqua_butm'], type='float', alias='tqua_butm') }},
        {{ trx_json_extract('src', ['tqua_buvl'], type='float', alias='tqua_buvl') }},
        {{ trx_json_extract('src', ['tqua_buwg'], type='float', alias='tqua_buwg') }},
        {{ trx_json_extract('src', ['deleted'], type='boolean', alias='deleted') }},
        {{ trx_json_extract('src', ['username'], type='varchar', alias='username') }},
        {{ trx_json_extract('src', ['timestamp'], type='timestamp_ntz', alias='timestamp') }},
        {{ trx_json_extract('src', ['sequencenumber'], type='int', alias='sequencenumber') }},
        {{ trx_etl_columns() }}
    from {{ source('source_landing_ln', 'ln_qmptc115') }}
)

select
    *,
    {{ trx_audit_columns() }},
    {{ trx_audit_surrogate(columns=['compnr', 'iorn', 'saml', 'pono', 'srno']) }}
from cte_flattened
{% if is_incremental() %}
where etl_load_datetime > (select max(etl_load_datetime) from {{ this }})
{% endif %}
{{ trx_current_state(order_by=['sequencenumber']) }}

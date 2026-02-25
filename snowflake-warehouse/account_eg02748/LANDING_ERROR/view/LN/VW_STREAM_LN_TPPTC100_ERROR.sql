CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPTC100_ERROR AS SELECT src, 'LN_TPPTC100' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrt), '0'), 38, 10) is null and 
                    src:afrt is not null) THEN
                    'afrt contains a non-numeric value : \'' || AS_VARCHAR(src:afrt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) THEN
                    'blbl contains a non-numeric value : \'' || AS_VARCHAR(src:blbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cape), '0'), 38, 10) is null and 
                    src:cape is not null) THEN
                    'cape contains a non-numeric value : \'' || AS_VARCHAR(src:cape) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccat_ref_compnr is not null) THEN
                    'ccat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccst), '0'), 38, 10) is null and 
                    src:ccst is not null) THEN
                    'ccst contains a non-numeric value : \'' || AS_VARCHAR(src:ccst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctf), '0'), 38, 10) is null and 
                    src:cctf is not null) THEN
                    'cctf contains a non-numeric value : \'' || AS_VARCHAR(src:cctf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctr), '0'), 38, 10) is null and 
                    src:cctr is not null) THEN
                    'cctr contains a non-numeric value : \'' || AS_VARCHAR(src:cctr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) THEN
                    'cprj_cstl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cstl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is null and 
                    src:csec_ref_compnr is not null) THEN
                    'csec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspt), '0'), 38, 10) is null and 
                    src:cspt is not null) THEN
                    'cspt contains a non-numeric value : \'' || AS_VARCHAR(src:cspt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is null and 
                    src:cstg_ref_compnr is not null) THEN
                    'cstg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cstg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) THEN
                    'cstv contains a non-numeric value : \'' || AS_VARCHAR(src:cstv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) THEN
                    'cuni_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuni_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) THEN
                    'cuti_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuti_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) THEN
                    'cuvc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuvc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dldt), '1900-01-01')) is null and 
                    src:dldt is not null) THEN
                    'dldt contains a non-timestamp value : \'' || AS_VARCHAR(src:dldt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dwar_ref_compnr), '0'), 38, 10) is null and 
                    src:dwar_ref_compnr is not null) THEN
                    'dwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:freq), '0'), 38, 10) is null and 
                    src:freq is not null) THEN
                    'freq contains a non-numeric value : \'' || AS_VARCHAR(src:freq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gris), '0'), 38, 10) is null and 
                    src:gris is not null) THEN
                    'gris contains a non-numeric value : \'' || AS_VARCHAR(src:gris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbyn), '0'), 38, 10) is null and 
                    src:hbyn is not null) THEN
                    'hbyn contains a non-numeric value : \'' || AS_VARCHAR(src:hbyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:llnr), '0'), 38, 10) is null and 
                    src:llnr is not null) THEN
                    'llnr contains a non-numeric value : \'' || AS_VARCHAR(src:llnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltpr), '1900-01-01')) is null and 
                    src:ltpr is not null) THEN
                    'ltpr contains a non-timestamp value : \'' || AS_VARCHAR(src:ltpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lvps), '0'), 38, 10) is null and 
                    src:lvps is not null) THEN
                    'lvps contains a non-numeric value : \'' || AS_VARCHAR(src:lvps) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ohdt), '1900-01-01')) is null and 
                    src:ohdt is not null) THEN
                    'ohdt contains a non-timestamp value : \'' || AS_VARCHAR(src:ohdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prin), '0'), 38, 10) is null and 
                    src:prin is not null) THEN
                    'prin contains a non-numeric value : \'' || AS_VARCHAR(src:prin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) THEN
                    'pris contains a non-numeric value : \'' || AS_VARCHAR(src:pris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpf), '0'), 38, 10) is null and 
                    src:prpf is not null) THEN
                    'prpf contains a non-numeric value : \'' || AS_VARCHAR(src:prpf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prps), '0'), 38, 10) is null and 
                    src:prps is not null) THEN
                    'prps contains a non-numeric value : \'' || AS_VARCHAR(src:prps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsp), '0'), 38, 10) is null and 
                    src:prsp is not null) THEN
                    'prsp contains a non-numeric value : \'' || AS_VARCHAR(src:prsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prss), '0'), 38, 10) is null and 
                    src:prss is not null) THEN
                    'prss contains a non-numeric value : \'' || AS_VARCHAR(src:prss) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pwar_ref_compnr), '0'), 38, 10) is null and 
                    src:pwar_ref_compnr is not null) THEN
                    'pwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qanc), '0'), 38, 10) is null and 
                    src:qanc is not null) THEN
                    'qanc contains a non-numeric value : \'' || AS_VARCHAR(src:qanc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qans), '0'), 38, 10) is null and 
                    src:qans is not null) THEN
                    'qans contains a non-numeric value : \'' || AS_VARCHAR(src:qans) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qust), '0'), 38, 10) is null and 
                    src:qust is not null) THEN
                    'qust contains a non-numeric value : \'' || AS_VARCHAR(src:qust) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qutm), '0'), 38, 10) is null and 
                    src:qutm is not null) THEN
                    'qutm contains a non-numeric value : \'' || AS_VARCHAR(src:qutm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) THEN
                    'rcod_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcod_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdas), '1900-01-01')) is null and 
                    src:rdas is not null) THEN
                    'rdas contains a non-timestamp value : \'' || AS_VARCHAR(src:rdas) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdse), '1900-01-01')) is null and 
                    src:rdse is not null) THEN
                    'rdse contains a non-timestamp value : \'' || AS_VARCHAR(src:rdse) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsa_1), '0'), 38, 10) is null and 
                    src:rfsa_1 is not null) THEN
                    'rfsa_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rfsa_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsa_2), '0'), 38, 10) is null and 
                    src:rfsa_2 is not null) THEN
                    'rfsa_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rfsa_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsa_3), '0'), 38, 10) is null and 
                    src:rfsa_3 is not null) THEN
                    'rfsa_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rfsa_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfse_1), '0'), 38, 10) is null and 
                    src:rfse_1 is not null) THEN
                    'rfse_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rfse_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfse_2), '0'), 38, 10) is null and 
                    src:rfse_2 is not null) THEN
                    'rfse_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rfse_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfse_3), '0'), 38, 10) is null and 
                    src:rfse_3 is not null) THEN
                    'rfse_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rfse_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_1), '0'), 38, 10) is null and 
                    src:rtsa_1 is not null) THEN
                    'rtsa_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtsa_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_2), '0'), 38, 10) is null and 
                    src:rtsa_2 is not null) THEN
                    'rtsa_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtsa_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_3), '0'), 38, 10) is null and 
                    src:rtsa_3 is not null) THEN
                    'rtsa_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtsa_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtse_1), '0'), 38, 10) is null and 
                    src:rtse_1 is not null) THEN
                    'rtse_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rtse_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtse_2), '0'), 38, 10) is null and 
                    src:rtse_2 is not null) THEN
                    'rtse_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rtse_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtse_3), '0'), 38, 10) is null and 
                    src:rtse_3 is not null) THEN
                    'rtse_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rtse_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacu_ref_compnr), '0'), 38, 10) is null and 
                    src:sacu_ref_compnr is not null) THEN
                    'sacu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sacu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:secu_ref_compnr), '0'), 38, 10) is null and 
                    src:secu_ref_compnr is not null) THEN
                    'secu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:secu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:setl), '0'), 38, 10) is null and 
                    src:setl is not null) THEN
                    'setl contains a non-numeric value : \'' || AS_VARCHAR(src:setl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) THEN
                    'stdt contains a non-timestamp value : \'' || AS_VARCHAR(src:stdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txsp), '0'), 38, 10) is null and 
                    src:txsp is not null) THEN
                    'txsp contains a non-numeric value : \'' || AS_VARCHAR(src:txsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wast), '0'), 38, 10) is null and 
                    src:wast is not null) THEN
                    'wast contains a non-numeric value : \'' || AS_VARCHAR(src:wast) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null) THEN
                'sequencenumber contains a non-timestamp value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
                etl_load_datetime,
                etl_load_metadatafilename
                FROM 
                (
                select 
                    src,
                    etl_load_datetime,
                    etl_load_metadatafilename
                    from
                    (
                        SELECT
        
                            
            src,
            etl_load_datetime,
            etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
                                    
                src:compnr,
                src:cprj,
                src:cspa  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPTC100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrt), '0'), 38, 10) is null and 
                    src:afrt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blbl), '0'), 38, 10) is null and 
                    src:blbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cape), '0'), 38, 10) is null and 
                    src:cape is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccat_ref_compnr), '0'), 38, 10) is null and 
                    src:ccat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccst), '0'), 38, 10) is null and 
                    src:ccst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctf), '0'), 38, 10) is null and 
                    src:cctf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctr), '0'), 38, 10) is null and 
                    src:cctr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cstl_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cstl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csec_ref_compnr), '0'), 38, 10) is null and 
                    src:csec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspt), '0'), 38, 10) is null and 
                    src:cspt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstg_ref_compnr), '0'), 38, 10) is null and 
                    src:cstg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cstv), '0'), 38, 10) is null and 
                    src:cstv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuni_ref_compnr), '0'), 38, 10) is null and 
                    src:cuni_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuti_ref_compnr), '0'), 38, 10) is null and 
                    src:cuti_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dldt), '1900-01-01')) is null and 
                    src:dldt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dwar_ref_compnr), '0'), 38, 10) is null and 
                    src:dwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:freq), '0'), 38, 10) is null and 
                    src:freq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gris), '0'), 38, 10) is null and 
                    src:gris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hbyn), '0'), 38, 10) is null and 
                    src:hbyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:llnr), '0'), 38, 10) is null and 
                    src:llnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltpr), '1900-01-01')) is null and 
                    src:ltpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lvps), '0'), 38, 10) is null and 
                    src:lvps is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ohdt), '1900-01-01')) is null and 
                    src:ohdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prin), '0'), 38, 10) is null and 
                    src:prin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prpf), '0'), 38, 10) is null and 
                    src:prpf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prps), '0'), 38, 10) is null and 
                    src:prps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsp), '0'), 38, 10) is null and 
                    src:prsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prss), '0'), 38, 10) is null and 
                    src:prss is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pwar_ref_compnr), '0'), 38, 10) is null and 
                    src:pwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qanc), '0'), 38, 10) is null and 
                    src:qanc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qans), '0'), 38, 10) is null and 
                    src:qans is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qust), '0'), 38, 10) is null and 
                    src:qust is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qutm), '0'), 38, 10) is null and 
                    src:qutm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcod_ref_compnr), '0'), 38, 10) is null and 
                    src:rcod_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdas), '1900-01-01')) is null and 
                    src:rdas is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rdse), '1900-01-01')) is null and 
                    src:rdse is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsa_1), '0'), 38, 10) is null and 
                    src:rfsa_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsa_2), '0'), 38, 10) is null and 
                    src:rfsa_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsa_3), '0'), 38, 10) is null and 
                    src:rfsa_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfse_1), '0'), 38, 10) is null and 
                    src:rfse_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfse_2), '0'), 38, 10) is null and 
                    src:rfse_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfse_3), '0'), 38, 10) is null and 
                    src:rfse_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_1), '0'), 38, 10) is null and 
                    src:rtsa_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_2), '0'), 38, 10) is null and 
                    src:rtsa_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtsa_3), '0'), 38, 10) is null and 
                    src:rtsa_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtse_1), '0'), 38, 10) is null and 
                    src:rtse_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtse_2), '0'), 38, 10) is null and 
                    src:rtse_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtse_3), '0'), 38, 10) is null and 
                    src:rtse_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sacu_ref_compnr), '0'), 38, 10) is null and 
                    src:sacu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:secu_ref_compnr), '0'), 38, 10) is null and 
                    src:secu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:setl), '0'), 38, 10) is null and 
                    src:setl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txsp), '0'), 38, 10) is null and 
                    src:txsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wast), '0'), 38, 10) is null and 
                    src:wast is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)
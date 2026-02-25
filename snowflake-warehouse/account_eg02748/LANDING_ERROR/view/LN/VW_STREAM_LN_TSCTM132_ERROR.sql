CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSCTM132_ERROR AS SELECT src, 'LN_TSCTM132' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alco_1), '0'), 38, 10) is null and 
                    src:alco_1 is not null) THEN
                    'alco_1 contains a non-numeric value : \'' || AS_VARCHAR(src:alco_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alco_2), '0'), 38, 10) is null and 
                    src:alco_2 is not null) THEN
                    'alco_2 contains a non-numeric value : \'' || AS_VARCHAR(src:alco_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alco_3), '0'), 38, 10) is null and 
                    src:alco_3 is not null) THEN
                    'alco_3 contains a non-numeric value : \'' || AS_VARCHAR(src:alco_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alco_cntc), '0'), 38, 10) is null and 
                    src:alco_cntc is not null) THEN
                    'alco_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:alco_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alco_dwhc), '0'), 38, 10) is null and 
                    src:alco_dwhc is not null) THEN
                    'alco_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:alco_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alco_refc), '0'), 38, 10) is null and 
                    src:alco_refc is not null) THEN
                    'alco_refc contains a non-numeric value : \'' || AS_VARCHAR(src:alco_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alhr), '0'), 38, 10) is null and 
                    src:alhr is not null) THEN
                    'alhr contains a non-numeric value : \'' || AS_VARCHAR(src:alhr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alhr_butm), '0'), 38, 10) is null and 
                    src:alhr_butm is not null) THEN
                    'alhr_butm contains a non-numeric value : \'' || AS_VARCHAR(src:alhr_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alsa), '0'), 38, 10) is null and 
                    src:alsa is not null) THEN
                    'alsa contains a non-numeric value : \'' || AS_VARCHAR(src:alsa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alsa_dwhc), '0'), 38, 10) is null and 
                    src:alsa_dwhc is not null) THEN
                    'alsa_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:alsa_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alsa_homc), '0'), 38, 10) is null and 
                    src:alsa_homc is not null) THEN
                    'alsa_homc contains a non-numeric value : \'' || AS_VARCHAR(src:alsa_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alsa_refc), '0'), 38, 10) is null and 
                    src:alsa_refc is not null) THEN
                    'alsa_refc contains a non-numeric value : \'' || AS_VARCHAR(src:alsa_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alsa_rpc1), '0'), 38, 10) is null and 
                    src:alsa_rpc1 is not null) THEN
                    'alsa_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:alsa_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alsa_rpc2), '0'), 38, 10) is null and 
                    src:alsa_rpc2 is not null) THEN
                    'alsa_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:alsa_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) THEN
                    'amnt contains a non-numeric value : \'' || AS_VARCHAR(src:amnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_dwhc), '0'), 38, 10) is null and 
                    src:amnt_dwhc is not null) THEN
                    'amnt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_homc), '0'), 38, 10) is null and 
                    src:amnt_homc is not null) THEN
                    'amnt_homc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_refc), '0'), 38, 10) is null and 
                    src:amnt_refc is not null) THEN
                    'amnt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc1), '0'), 38, 10) is null and 
                    src:amnt_rpc1 is not null) THEN
                    'amnt_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc2), '0'), 38, 10) is null and 
                    src:amnt_rpc2 is not null) THEN
                    'amnt_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amnt_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_1), '0'), 38, 10) is null and 
                    src:camt_1 is not null) THEN
                    'camt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_2), '0'), 38, 10) is null and 
                    src:camt_2 is not null) THEN
                    'camt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_3), '0'), 38, 10) is null and 
                    src:camt_3 is not null) THEN
                    'camt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_cntc), '0'), 38, 10) is null and 
                    src:camt_cntc is not null) THEN
                    'camt_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_dwhc), '0'), 38, 10) is null and 
                    src:camt_dwhc is not null) THEN
                    'camt_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_refc), '0'), 38, 10) is null and 
                    src:camt_refc is not null) THEN
                    'camt_refc contains a non-numeric value : \'' || AS_VARCHAR(src:camt_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccmp_ref_compnr), '0'), 38, 10) is null and 
                    src:ccmp_ref_compnr is not null) THEN
                    'ccmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccmp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is null and 
                    src:cctp_ref_compnr is not null) THEN
                    'cctp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cctp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) THEN
                    'cfln contains a non-numeric value : \'' || AS_VARCHAR(src:cfln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chlt_ref_compnr), '0'), 38, 10) is null and 
                    src:chlt_ref_compnr is not null) THEN
                    'chlt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:chlt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clrt_ref_compnr), '0'), 38, 10) is null and 
                    src:clrt_ref_compnr is not null) THEN
                    'clrt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clrt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp), '0'), 38, 10) is null and 
                    src:cotp is not null) THEN
                    'cotp contains a non-numeric value : \'' || AS_VARCHAR(src:cotp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cseq), '0'), 38, 10) is null and 
                    src:cseq is not null) THEN
                    'cseq contains a non-numeric value : \'' || AS_VARCHAR(src:cseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctsk_ref_compnr), '0'), 38, 10) is null and 
                    src:ctsk_ref_compnr is not null) THEN
                    'ctsk_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctsk_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dspc), '0'), 38, 10) is null and 
                    src:dspc is not null) THEN
                    'dspc contains a non-numeric value : \'' || AS_VARCHAR(src:dspc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eahr), '0'), 38, 10) is null and 
                    src:eahr is not null) THEN
                    'eahr contains a non-numeric value : \'' || AS_VARCHAR(src:eahr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eahr_butm), '0'), 38, 10) is null and 
                    src:eahr_butm is not null) THEN
                    'eahr_butm contains a non-numeric value : \'' || AS_VARCHAR(src:eahr_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ealc_1), '0'), 38, 10) is null and 
                    src:ealc_1 is not null) THEN
                    'ealc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ealc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ealc_2), '0'), 38, 10) is null and 
                    src:ealc_2 is not null) THEN
                    'ealc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ealc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ealc_3), '0'), 38, 10) is null and 
                    src:ealc_3 is not null) THEN
                    'ealc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ealc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ealc_cntc), '0'), 38, 10) is null and 
                    src:ealc_cntc is not null) THEN
                    'ealc_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:ealc_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ealc_dwhc), '0'), 38, 10) is null and 
                    src:ealc_dwhc is not null) THEN
                    'ealc_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:ealc_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ealc_refc), '0'), 38, 10) is null and 
                    src:ealc_refc is not null) THEN
                    'ealc_refc contains a non-numeric value : \'' || AS_VARCHAR(src:ealc_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eals), '0'), 38, 10) is null and 
                    src:eals is not null) THEN
                    'eals contains a non-numeric value : \'' || AS_VARCHAR(src:eals) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eals_dwhc), '0'), 38, 10) is null and 
                    src:eals_dwhc is not null) THEN
                    'eals_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:eals_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eals_homc), '0'), 38, 10) is null and 
                    src:eals_homc is not null) THEN
                    'eals_homc contains a non-numeric value : \'' || AS_VARCHAR(src:eals_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eals_refc), '0'), 38, 10) is null and 
                    src:eals_refc is not null) THEN
                    'eals_refc contains a non-numeric value : \'' || AS_VARCHAR(src:eals_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eals_rpc1), '0'), 38, 10) is null and 
                    src:eals_rpc1 is not null) THEN
                    'eals_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:eals_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eals_rpc2), '0'), 38, 10) is null and 
                    src:eals_rpc2 is not null) THEN
                    'eals_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:eals_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrbt), '0'), 38, 10) is null and 
                    src:nrbt is not null) THEN
                    'nrbt contains a non-numeric value : \'' || AS_VARCHAR(src:nrbt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrhr), '0'), 38, 10) is null and 
                    src:nrhr is not null) THEN
                    'nrhr contains a non-numeric value : \'' || AS_VARCHAR(src:nrhr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrhr_butm), '0'), 38, 10) is null and 
                    src:nrhr_butm is not null) THEN
                    'nrhr_butm contains a non-numeric value : \'' || AS_VARCHAR(src:nrhr_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_1), '0'), 38, 10) is null and 
                    src:spco_1 is not null) THEN
                    'spco_1 contains a non-numeric value : \'' || AS_VARCHAR(src:spco_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_2), '0'), 38, 10) is null and 
                    src:spco_2 is not null) THEN
                    'spco_2 contains a non-numeric value : \'' || AS_VARCHAR(src:spco_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_3), '0'), 38, 10) is null and 
                    src:spco_3 is not null) THEN
                    'spco_3 contains a non-numeric value : \'' || AS_VARCHAR(src:spco_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_cntc), '0'), 38, 10) is null and 
                    src:spco_cntc is not null) THEN
                    'spco_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:spco_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_dwhc), '0'), 38, 10) is null and 
                    src:spco_dwhc is not null) THEN
                    'spco_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:spco_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_refc), '0'), 38, 10) is null and 
                    src:spco_refc is not null) THEN
                    'spco_refc contains a non-numeric value : \'' || AS_VARCHAR(src:spco_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sphr), '0'), 38, 10) is null and 
                    src:sphr is not null) THEN
                    'sphr contains a non-numeric value : \'' || AS_VARCHAR(src:sphr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa), '0'), 38, 10) is null and 
                    src:spsa is not null) THEN
                    'spsa contains a non-numeric value : \'' || AS_VARCHAR(src:spsa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_dwhc), '0'), 38, 10) is null and 
                    src:spsa_dwhc is not null) THEN
                    'spsa_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:spsa_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_homc), '0'), 38, 10) is null and 
                    src:spsa_homc is not null) THEN
                    'spsa_homc contains a non-numeric value : \'' || AS_VARCHAR(src:spsa_homc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_refc), '0'), 38, 10) is null and 
                    src:spsa_refc is not null) THEN
                    'spsa_refc contains a non-numeric value : \'' || AS_VARCHAR(src:spsa_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_rpc1), '0'), 38, 10) is null and 
                    src:spsa_rpc1 is not null) THEN
                    'spsa_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:spsa_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_rpc2), '0'), 38, 10) is null and 
                    src:spsa_rpc2 is not null) THEN
                    'spsa_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:spsa_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) THEN
                    'term contains a non-numeric value : \'' || AS_VARCHAR(src:term) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term_cfln_cctp_cotp_nrbt_ref_compnr), '0'), 38, 10) is null and 
                    src:term_cfln_cctp_cotp_nrbt_ref_compnr is not null) THEN
                    'term_cfln_cctp_cotp_nrbt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:term_cfln_cctp_cotp_nrbt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term_cfln_ref_compnr), '0'), 38, 10) is null and 
                    src:term_cfln_ref_compnr is not null) THEN
                    'term_cfln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:term_cfln_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wrac_1), '0'), 38, 10) is null and 
                    src:wrac_1 is not null) THEN
                    'wrac_1 contains a non-numeric value : \'' || AS_VARCHAR(src:wrac_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wrac_2), '0'), 38, 10) is null and 
                    src:wrac_2 is not null) THEN
                    'wrac_2 contains a non-numeric value : \'' || AS_VARCHAR(src:wrac_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wrac_3), '0'), 38, 10) is null and 
                    src:wrac_3 is not null) THEN
                    'wrac_3 contains a non-numeric value : \'' || AS_VARCHAR(src:wrac_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wras), '0'), 38, 10) is null and 
                    src:wras is not null) THEN
                    'wras contains a non-numeric value : \'' || AS_VARCHAR(src:wras) || '\'' WHEN 
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
                                    
                src:cctp,
                src:term,
                src:cotp,
                src:cseq,
                src:compnr,
                src:cfln,
                src:nrbt  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCTM132 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alco_1), '0'), 38, 10) is null and 
                    src:alco_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alco_2), '0'), 38, 10) is null and 
                    src:alco_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alco_3), '0'), 38, 10) is null and 
                    src:alco_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alco_cntc), '0'), 38, 10) is null and 
                    src:alco_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alco_dwhc), '0'), 38, 10) is null and 
                    src:alco_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alco_refc), '0'), 38, 10) is null and 
                    src:alco_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alhr), '0'), 38, 10) is null and 
                    src:alhr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alhr_butm), '0'), 38, 10) is null and 
                    src:alhr_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alsa), '0'), 38, 10) is null and 
                    src:alsa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alsa_dwhc), '0'), 38, 10) is null and 
                    src:alsa_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alsa_homc), '0'), 38, 10) is null and 
                    src:alsa_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alsa_refc), '0'), 38, 10) is null and 
                    src:alsa_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alsa_rpc1), '0'), 38, 10) is null and 
                    src:alsa_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alsa_rpc2), '0'), 38, 10) is null and 
                    src:alsa_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_dwhc), '0'), 38, 10) is null and 
                    src:amnt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_homc), '0'), 38, 10) is null and 
                    src:amnt_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_refc), '0'), 38, 10) is null and 
                    src:amnt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc1), '0'), 38, 10) is null and 
                    src:amnt_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt_rpc2), '0'), 38, 10) is null and 
                    src:amnt_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_1), '0'), 38, 10) is null and 
                    src:camt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_2), '0'), 38, 10) is null and 
                    src:camt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_3), '0'), 38, 10) is null and 
                    src:camt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_cntc), '0'), 38, 10) is null and 
                    src:camt_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_dwhc), '0'), 38, 10) is null and 
                    src:camt_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_refc), '0'), 38, 10) is null and 
                    src:camt_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccmp_ref_compnr), '0'), 38, 10) is null and 
                    src:ccmp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cctp_ref_compnr), '0'), 38, 10) is null and 
                    src:cctp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chlt_ref_compnr), '0'), 38, 10) is null and 
                    src:chlt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clrt_ref_compnr), '0'), 38, 10) is null and 
                    src:clrt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp), '0'), 38, 10) is null and 
                    src:cotp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cseq), '0'), 38, 10) is null and 
                    src:cseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctsk_ref_compnr), '0'), 38, 10) is null and 
                    src:ctsk_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dspc), '0'), 38, 10) is null and 
                    src:dspc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eahr), '0'), 38, 10) is null and 
                    src:eahr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eahr_butm), '0'), 38, 10) is null and 
                    src:eahr_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ealc_1), '0'), 38, 10) is null and 
                    src:ealc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ealc_2), '0'), 38, 10) is null and 
                    src:ealc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ealc_3), '0'), 38, 10) is null and 
                    src:ealc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ealc_cntc), '0'), 38, 10) is null and 
                    src:ealc_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ealc_dwhc), '0'), 38, 10) is null and 
                    src:ealc_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ealc_refc), '0'), 38, 10) is null and 
                    src:ealc_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eals), '0'), 38, 10) is null and 
                    src:eals is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eals_dwhc), '0'), 38, 10) is null and 
                    src:eals_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eals_homc), '0'), 38, 10) is null and 
                    src:eals_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eals_refc), '0'), 38, 10) is null and 
                    src:eals_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eals_rpc1), '0'), 38, 10) is null and 
                    src:eals_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eals_rpc2), '0'), 38, 10) is null and 
                    src:eals_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrbt), '0'), 38, 10) is null and 
                    src:nrbt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrhr), '0'), 38, 10) is null and 
                    src:nrhr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrhr_butm), '0'), 38, 10) is null and 
                    src:nrhr_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_1), '0'), 38, 10) is null and 
                    src:spco_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_2), '0'), 38, 10) is null and 
                    src:spco_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_3), '0'), 38, 10) is null and 
                    src:spco_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_cntc), '0'), 38, 10) is null and 
                    src:spco_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_dwhc), '0'), 38, 10) is null and 
                    src:spco_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spco_refc), '0'), 38, 10) is null and 
                    src:spco_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sphr), '0'), 38, 10) is null and 
                    src:sphr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa), '0'), 38, 10) is null and 
                    src:spsa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_dwhc), '0'), 38, 10) is null and 
                    src:spsa_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_homc), '0'), 38, 10) is null and 
                    src:spsa_homc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_refc), '0'), 38, 10) is null and 
                    src:spsa_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_rpc1), '0'), 38, 10) is null and 
                    src:spsa_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsa_rpc2), '0'), 38, 10) is null and 
                    src:spsa_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term), '0'), 38, 10) is null and 
                    src:term is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term_cfln_cctp_cotp_nrbt_ref_compnr), '0'), 38, 10) is null and 
                    src:term_cfln_cctp_cotp_nrbt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:term_cfln_ref_compnr), '0'), 38, 10) is null and 
                    src:term_cfln_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wrac_1), '0'), 38, 10) is null and 
                    src:wrac_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wrac_2), '0'), 38, 10) is null and 
                    src:wrac_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wrac_3), '0'), 38, 10) is null and 
                    src:wrac_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wras), '0'), 38, 10) is null and 
                    src:wras is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)
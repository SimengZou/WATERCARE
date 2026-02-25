CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCCOM110_ERROR AS SELECT src, 'LN_TCCOM110' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aalg), '0'), 38, 10) is null and 
                    src:aalg is not null) THEN
                    'aalg contains a non-numeric value : \'' || AS_VARCHAR(src:aalg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aapr), '0'), 38, 10) is null and 
                    src:aapr is not null) THEN
                    'aapr contains a non-numeric value : \'' || AS_VARCHAR(src:aapr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaps), '0'), 38, 10) is null and 
                    src:aaps is not null) THEN
                    'aaps contains a non-numeric value : \'' || AS_VARCHAR(src:aaps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ackx), '0'), 38, 10) is null and 
                    src:ackx is not null) THEN
                    'ackx contains a non-numeric value : \'' || AS_VARCHAR(src:ackx) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aprc), '0'), 38, 10) is null and 
                    src:aprc is not null) THEN
                    'aprc contains a non-numeric value : \'' || AS_VARCHAR(src:aprc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apsr), '0'), 38, 10) is null and 
                    src:apsr is not null) THEN
                    'apsr contains a non-numeric value : \'' || AS_VARCHAR(src:apsr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arcu_ref_compnr), '0'), 38, 10) is null and 
                    src:arcu_ref_compnr is not null) THEN
                    'arcu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:arcu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arev), '0'), 38, 10) is null and 
                    src:arev is not null) THEN
                    'arev contains a non-numeric value : \'' || AS_VARCHAR(src:arev) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asma), '0'), 38, 10) is null and 
                    src:asma is not null) THEN
                    'asma contains a non-numeric value : \'' || AS_VARCHAR(src:asma) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bppr_ref_compnr), '0'), 38, 10) is null and 
                    src:bppr_ref_compnr is not null) THEN
                    'bppr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bppr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpst), '0'), 38, 10) is null and 
                    src:bpst is not null) THEN
                    'bpst contains a non-numeric value : \'' || AS_VARCHAR(src:bpst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpus_ref_compnr), '0'), 38, 10) is null and 
                    src:bpus_ref_compnr is not null) THEN
                    'bpus_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpus_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cacf), '0'), 38, 10) is null and 
                    src:cacf is not null) THEN
                    'cacf contains a non-numeric value : \'' || AS_VARCHAR(src:cacf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cacs), '0'), 38, 10) is null and 
                    src:cacs is not null) THEN
                    'cacs contains a non-numeric value : \'' || AS_VARCHAR(src:cacs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbps_ref_compnr), '0'), 38, 10) is null and 
                    src:cbps_ref_compnr is not null) THEN
                    'cbps_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbps_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) THEN
                    'cbrn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbrn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbtp_ref_compnr), '0'), 38, 10) is null and 
                    src:cbtp_ref_compnr is not null) THEN
                    'cbtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) THEN
                    'ccal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccnt_ref_compnr), '0'), 38, 10) is null and 
                    src:ccnt_ref_compnr is not null) THEN
                    'ccnt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccnt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chak), '0'), 38, 10) is null and 
                    src:chak is not null) THEN
                    'chak contains a non-numeric value : \'' || AS_VARCHAR(src:chak) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chan_ref_compnr), '0'), 38, 10) is null and 
                    src:chan_ref_compnr is not null) THEN
                    'chan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:chan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) THEN
                    'clan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clgr_ref_compnr), '0'), 38, 10) is null and 
                    src:clgr_ref_compnr is not null) THEN
                    'clgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clgr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpls_ref_compnr), '0'), 38, 10) is null and 
                    src:cpls_ref_compnr is not null) THEN
                    'cpls_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpls_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) THEN
                    'creg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:creg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is null and 
                    src:crep_ref_compnr is not null) THEN
                    'crep_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crep_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspr), '0'), 38, 10) is null and 
                    src:cspr is not null) THEN
                    'cspr contains a non-numeric value : \'' || AS_VARCHAR(src:cspr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspr_ref_compnr), '0'), 38, 10) is null and 
                    src:cspr_ref_compnr is not null) THEN
                    'cspr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cspr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:endt), '1900-01-01')) is null and 
                    src:endt is not null) THEN
                    'endt contains a non-timestamp value : \'' || AS_VARCHAR(src:endt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frin), '0'), 38, 10) is null and 
                    src:frin is not null) THEN
                    'frin contains a non-numeric value : \'' || AS_VARCHAR(src:frin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incd_ref_compnr), '0'), 38, 10) is null and 
                    src:incd_ref_compnr is not null) THEN
                    'incd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:incd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) THEN
                    'itbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) THEN
                    'lcmp contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) THEN
                    'lcmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmsp), '0'), 38, 10) is null and 
                    src:lmsp is not null) THEN
                    'lmsp contains a non-numeric value : \'' || AS_VARCHAR(src:lmsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:macc), '0'), 38, 10) is null and 
                    src:macc is not null) THEN
                    'macc contains a non-numeric value : \'' || AS_VARCHAR(src:macc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mask_ref_compnr), '0'), 38, 10) is null and 
                    src:mask_ref_compnr is not null) THEN
                    'mask_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mask_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfr), '0'), 38, 10) is null and 
                    src:mcfr is not null) THEN
                    'mcfr contains a non-numeric value : \'' || AS_VARCHAR(src:mcfr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:noem), '0'), 38, 10) is null and 
                    src:noem is not null) THEN
                    'noem contains a non-numeric value : \'' || AS_VARCHAR(src:noem) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odis), '0'), 38, 10) is null and 
                    src:odis is not null) THEN
                    'odis contains a non-numeric value : \'' || AS_VARCHAR(src:odis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) THEN
                    'ofbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_stbp_ref_compnr is not null) THEN
                    'ofbp_stbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ofbp_stbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is null and 
                    src:osrp_ref_compnr is not null) THEN
                    'osrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:osrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmp), '0'), 38, 10) is null and 
                    src:pcmp is not null) THEN
                    'pcmp contains a non-numeric value : \'' || AS_VARCHAR(src:pcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pldd_ref_compnr), '0'), 38, 10) is null and 
                    src:pldd_ref_compnr is not null) THEN
                    'pldd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pldd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio), '0'), 38, 10) is null and 
                    src:prio is not null) THEN
                    'prio contains a non-numeric value : \'' || AS_VARCHAR(src:prio) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio_ref_compnr), '0'), 38, 10) is null and 
                    src:prio_ref_compnr is not null) THEN
                    'prio_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prio_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prms), '0'), 38, 10) is null and 
                    src:prms is not null) THEN
                    'prms contains a non-numeric value : \'' || AS_VARCHAR(src:prms) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdec_ref_compnr), '0'), 38, 10) is null and 
                    src:rdec_ref_compnr is not null) THEN
                    'rdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rinv), '0'), 38, 10) is null and 
                    src:rinv is not null) THEN
                    'rinv contains a non-numeric value : \'' || AS_VARCHAR(src:rinv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtng), '0'), 38, 10) is null and 
                    src:rtng is not null) THEN
                    'rtng contains a non-numeric value : \'' || AS_VARCHAR(src:rtng) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbil), '0'), 38, 10) is null and 
                    src:sbil is not null) THEN
                    'sbil contains a non-numeric value : \'' || AS_VARCHAR(src:sbil) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) THEN
                    'scon contains a non-numeric value : \'' || AS_VARCHAR(src:scon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scqq), '0'), 38, 10) is null and 
                    src:scqq is not null) THEN
                    'scqq contains a non-numeric value : \'' || AS_VARCHAR(src:scqq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sscc), '0'), 38, 10) is null and 
                    src:sscc is not null) THEN
                    'sscc contains a non-numeric value : \'' || AS_VARCHAR(src:sscc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) THEN
                    'stbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:stbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) THEN
                    'stdt contains a non-timestamp value : \'' || AS_VARCHAR(src:stdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ster_ref_compnr), '0'), 38, 10) is null and 
                    src:ster_ref_compnr is not null) THEN
                    'ster_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ster_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucnd), '0'), 38, 10) is null and 
                    src:ucnd is not null) THEN
                    'ucnd contains a non-numeric value : \'' || AS_VARCHAR(src:ucnd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:umsp), '0'), 38, 10) is null and 
                    src:umsp is not null) THEN
                    'umsp contains a non-numeric value : \'' || AS_VARCHAR(src:umsp) || '\'' WHEN 
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
                src:ofbp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCCOM110 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aalg), '0'), 38, 10) is null and 
                    src:aalg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aapr), '0'), 38, 10) is null and 
                    src:aapr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaps), '0'), 38, 10) is null and 
                    src:aaps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ackx), '0'), 38, 10) is null and 
                    src:ackx is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aprc), '0'), 38, 10) is null and 
                    src:aprc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apsr), '0'), 38, 10) is null and 
                    src:apsr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arcu_ref_compnr), '0'), 38, 10) is null and 
                    src:arcu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arev), '0'), 38, 10) is null and 
                    src:arev is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asma), '0'), 38, 10) is null and 
                    src:asma is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bppr_ref_compnr), '0'), 38, 10) is null and 
                    src:bppr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpst), '0'), 38, 10) is null and 
                    src:bpst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpus_ref_compnr), '0'), 38, 10) is null and 
                    src:bpus_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cacf), '0'), 38, 10) is null and 
                    src:cacf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cacs), '0'), 38, 10) is null and 
                    src:cacs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbps_ref_compnr), '0'), 38, 10) is null and 
                    src:cbps_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrn_ref_compnr), '0'), 38, 10) is null and 
                    src:cbrn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbtp_ref_compnr), '0'), 38, 10) is null and 
                    src:cbtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccnt_ref_compnr), '0'), 38, 10) is null and 
                    src:ccnt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chak), '0'), 38, 10) is null and 
                    src:chak is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:chan_ref_compnr), '0'), 38, 10) is null and 
                    src:chan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clgr_ref_compnr), '0'), 38, 10) is null and 
                    src:clgr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpls_ref_compnr), '0'), 38, 10) is null and 
                    src:cpls_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crep_ref_compnr), '0'), 38, 10) is null and 
                    src:crep_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspr), '0'), 38, 10) is null and 
                    src:cspr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspr_ref_compnr), '0'), 38, 10) is null and 
                    src:cspr_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:endt), '1900-01-01')) is null and 
                    src:endt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frin), '0'), 38, 10) is null and 
                    src:frin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incd_ref_compnr), '0'), 38, 10) is null and 
                    src:incd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itbp_ref_compnr), '0'), 38, 10) is null and 
                    src:itbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmsp), '0'), 38, 10) is null and 
                    src:lmsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:macc), '0'), 38, 10) is null and 
                    src:macc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mask_ref_compnr), '0'), 38, 10) is null and 
                    src:mask_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfr), '0'), 38, 10) is null and 
                    src:mcfr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:noem), '0'), 38, 10) is null and 
                    src:noem is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odis), '0'), 38, 10) is null and 
                    src:odis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ofbp_stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ofbp_stbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osrp_ref_compnr), '0'), 38, 10) is null and 
                    src:osrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmp), '0'), 38, 10) is null and 
                    src:pcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pldd_ref_compnr), '0'), 38, 10) is null and 
                    src:pldd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio), '0'), 38, 10) is null and 
                    src:prio is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio_ref_compnr), '0'), 38, 10) is null and 
                    src:prio_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prms), '0'), 38, 10) is null and 
                    src:prms is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdec_ref_compnr), '0'), 38, 10) is null and 
                    src:rdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rinv), '0'), 38, 10) is null and 
                    src:rinv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtng), '0'), 38, 10) is null and 
                    src:rtng is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbil), '0'), 38, 10) is null and 
                    src:sbil is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scqq), '0'), 38, 10) is null and 
                    src:scqq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sscc), '0'), 38, 10) is null and 
                    src:sscc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbp_ref_compnr), '0'), 38, 10) is null and 
                    src:stbp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ster_ref_compnr), '0'), 38, 10) is null and 
                    src:ster_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucnd), '0'), 38, 10) is null and 
                    src:ucnd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:umsp), '0'), 38, 10) is null and 
                    src:umsp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)
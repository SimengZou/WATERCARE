CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCCOM120_ERROR AS SELECT src, 'LN_TCCOM120' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ackx), '0'), 38, 10) is null and 
                    src:ackx is not null) THEN
                    'ackx contains a non-numeric value : \'' || AS_VARCHAR(src:ackx) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrb), '0'), 38, 10) is null and 
                    src:afrb is not null) THEN
                    'afrb contains a non-numeric value : \'' || AS_VARCHAR(src:afrb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asbs), '0'), 38, 10) is null and 
                    src:asbs is not null) THEN
                    'asbs contains a non-numeric value : \'' || AS_VARCHAR(src:asbs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bppr_ref_compnr), '0'), 38, 10) is null and 
                    src:bppr_ref_compnr is not null) THEN
                    'bppr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bppr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpst), '0'), 38, 10) is null and 
                    src:bpst is not null) THEN
                    'bpst contains a non-numeric value : \'' || AS_VARCHAR(src:bpst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpus_ref_compnr), '0'), 38, 10) is null and 
                    src:bpus_ref_compnr is not null) THEN
                    'bpus_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpus_ref_compnr) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccon_ref_compnr), '0'), 38, 10) is null and 
                    src:ccon_ref_compnr is not null) THEN
                    'ccon_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccon_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) THEN
                    'cdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) THEN
                    'clan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clgr_ref_compnr), '0'), 38, 10) is null and 
                    src:clgr_ref_compnr is not null) THEN
                    'clgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clgr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cplp_ref_compnr), '0'), 38, 10) is null and 
                    src:cplp_ref_compnr is not null) THEN
                    'cplp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cplp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) THEN
                    'creg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:creg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crrf), '0'), 38, 10) is null and 
                    src:crrf is not null) THEN
                    'crrf contains a non-numeric value : \'' || AS_VARCHAR(src:crrf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlpe), '0'), 38, 10) is null and 
                    src:dlpe is not null) THEN
                    'dlpe contains a non-numeric value : \'' || AS_VARCHAR(src:dlpe) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlpu), '0'), 38, 10) is null and 
                    src:dlpu is not null) THEN
                    'dlpu contains a non-numeric value : \'' || AS_VARCHAR(src:dlpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dset_ref_compnr), '0'), 38, 10) is null and 
                    src:dset_ref_compnr is not null) THEN
                    'dset_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dset_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:encd), '0'), 38, 10) is null and 
                    src:encd is not null) THEN
                    'encd contains a non-numeric value : \'' || AS_VARCHAR(src:encd) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:endt), '1900-01-01')) is null and 
                    src:endt is not null) THEN
                    'endt contains a non-timestamp value : \'' || AS_VARCHAR(src:endt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frin), '0'), 38, 10) is null and 
                    src:frin is not null) THEN
                    'frin contains a non-numeric value : \'' || AS_VARCHAR(src:frin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) THEN
                    'ifbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ifbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itgr_ref_compnr), '0'), 38, 10) is null and 
                    src:itgr_ref_compnr is not null) THEN
                    'itgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:itgr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) THEN
                    'lcmp contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) THEN
                    'lcmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfr), '0'), 38, 10) is null and 
                    src:mcfr is not null) THEN
                    'mcfr contains a non-numeric value : \'' || AS_VARCHAR(src:mcfr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mesv), '0'), 38, 10) is null and 
                    src:mesv is not null) THEN
                    'mesv contains a non-numeric value : \'' || AS_VARCHAR(src:mesv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nasu_ref_compnr), '0'), 38, 10) is null and 
                    src:nasu_ref_compnr is not null) THEN
                    'nasu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:nasu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odis), '0'), 38, 10) is null and 
                    src:odis is not null) THEN
                    'odis contains a non-numeric value : \'' || AS_VARCHAR(src:odis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paft), '0'), 38, 10) is null and 
                    src:paft is not null) THEN
                    'paft contains a non-numeric value : \'' || AS_VARCHAR(src:paft) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) THEN
                    'ptpa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptpa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:raur), '0'), 38, 10) is null and 
                    src:raur is not null) THEN
                    'raur contains a non-numeric value : \'' || AS_VARCHAR(src:raur) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdec_ref_compnr), '0'), 38, 10) is null and 
                    src:rdec_ref_compnr is not null) THEN
                    'rdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retp), '0'), 38, 10) is null and 
                    src:retp is not null) THEN
                    'retp contains a non-numeric value : \'' || AS_VARCHAR(src:retp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbdt), '0'), 38, 10) is null and 
                    src:sbdt is not null) THEN
                    'sbdt contains a non-numeric value : \'' || AS_VARCHAR(src:sbdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbil), '0'), 38, 10) is null and 
                    src:sbil is not null) THEN
                    'sbil contains a non-numeric value : \'' || AS_VARCHAR(src:sbil) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sbvf), '1900-01-01')) is null and 
                    src:sbvf is not null) THEN
                    'sbvf contains a non-timestamp value : \'' || AS_VARCHAR(src:sbvf) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sbvt), '1900-01-01')) is null and 
                    src:sbvt is not null) THEN
                    'sbvt contains a non-timestamp value : \'' || AS_VARCHAR(src:sbvt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scqq), '0'), 38, 10) is null and 
                    src:scqq is not null) THEN
                    'scqq contains a non-numeric value : \'' || AS_VARCHAR(src:scqq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) THEN
                    'sfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srtr_ref_compnr), '0'), 38, 10) is null and 
                    src:srtr_ref_compnr is not null) THEN
                    'srtr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:srtr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) THEN
                    'stdt contains a non-timestamp value : \'' || AS_VARCHAR(src:stdt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tira), '0'), 38, 10) is null and 
                    src:tira is not null) THEN
                    'tira contains a non-numeric value : \'' || AS_VARCHAR(src:tira) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucnd), '0'), 38, 10) is null and 
                    src:ucnd is not null) THEN
                    'ucnd contains a non-numeric value : \'' || AS_VARCHAR(src:ucnd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vryn), '0'), 38, 10) is null and 
                    src:vryn is not null) THEN
                    'vryn contains a non-numeric value : \'' || AS_VARCHAR(src:vryn) || '\'' WHEN 
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
                src:otbp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCCOM120 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ackx), '0'), 38, 10) is null and 
                    src:ackx is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrb), '0'), 38, 10) is null and 
                    src:afrb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asbs), '0'), 38, 10) is null and 
                    src:asbs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bppr_ref_compnr), '0'), 38, 10) is null and 
                    src:bppr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpst), '0'), 38, 10) is null and 
                    src:bpst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpus_ref_compnr), '0'), 38, 10) is null and 
                    src:bpus_ref_compnr is not null) or 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccon_ref_compnr), '0'), 38, 10) is null and 
                    src:ccon_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdec_ref_compnr), '0'), 38, 10) is null and 
                    src:cdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clgr_ref_compnr), '0'), 38, 10) is null and 
                    src:clgr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cplp_ref_compnr), '0'), 38, 10) is null and 
                    src:cplp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:creg_ref_compnr), '0'), 38, 10) is null and 
                    src:creg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crrf), '0'), 38, 10) is null and 
                    src:crrf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlpe), '0'), 38, 10) is null and 
                    src:dlpe is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dlpu), '0'), 38, 10) is null and 
                    src:dlpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dset_ref_compnr), '0'), 38, 10) is null and 
                    src:dset_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:encd), '0'), 38, 10) is null and 
                    src:encd is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:endt), '1900-01-01')) is null and 
                    src:endt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frin), '0'), 38, 10) is null and 
                    src:frin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itgr_ref_compnr), '0'), 38, 10) is null and 
                    src:itgr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcfr), '0'), 38, 10) is null and 
                    src:mcfr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mesv), '0'), 38, 10) is null and 
                    src:mesv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nasu_ref_compnr), '0'), 38, 10) is null and 
                    src:nasu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odis), '0'), 38, 10) is null and 
                    src:odis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paft), '0'), 38, 10) is null and 
                    src:paft is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptpa_ref_compnr), '0'), 38, 10) is null and 
                    src:ptpa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:raur), '0'), 38, 10) is null and 
                    src:raur is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdec_ref_compnr), '0'), 38, 10) is null and 
                    src:rdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retp), '0'), 38, 10) is null and 
                    src:retp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbdt), '0'), 38, 10) is null and 
                    src:sbdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbil), '0'), 38, 10) is null and 
                    src:sbil is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sbvf), '1900-01-01')) is null and 
                    src:sbvf is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sbvt), '1900-01-01')) is null and 
                    src:sbvt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scqq), '0'), 38, 10) is null and 
                    src:scqq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srtr_ref_compnr), '0'), 38, 10) is null and 
                    src:srtr_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tira), '0'), 38, 10) is null and 
                    src:tira is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucnd), '0'), 38, 10) is null and 
                    src:ucnd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vryn), '0'), 38, 10) is null and 
                    src:vryn is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)
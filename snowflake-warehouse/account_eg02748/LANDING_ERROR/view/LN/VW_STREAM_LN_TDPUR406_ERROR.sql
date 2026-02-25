CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDPUR406_ERROR AS SELECT src, 'LN_TDPUR406' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrw_ref_compnr), '0'), 38, 10) is null and 
                    src:afrw_ref_compnr is not null) THEN
                    'afrw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:afrw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld), '0'), 38, 10) is null and 
                    src:amld is not null) THEN
                    'amld contains a non-numeric value : \'' || AS_VARCHAR(src:amld) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod), '0'), 38, 10) is null and 
                    src:amod is not null) THEN
                    'amod contains a non-numeric value : \'' || AS_VARCHAR(src:amod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arej), '0'), 38, 10) is null and 
                    src:arej is not null) THEN
                    'arej contains a non-numeric value : \'' || AS_VARCHAR(src:arej) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgxc), '0'), 38, 10) is null and 
                    src:bgxc is not null) THEN
                    'bgxc contains a non-numeric value : \'' || AS_VARCHAR(src:bgxc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) THEN
                    'cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmnf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:conf), '0'), 38, 10) is null and 
                    src:conf is not null) THEN
                    'conf contains a non-numeric value : \'' || AS_VARCHAR(src:conf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coop_1), '0'), 38, 10) is null and 
                    src:coop_1 is not null) THEN
                    'coop_1 contains a non-numeric value : \'' || AS_VARCHAR(src:coop_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coop_2), '0'), 38, 10) is null and 
                    src:coop_2 is not null) THEN
                    'coop_2 contains a non-numeric value : \'' || AS_VARCHAR(src:coop_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coop_3), '0'), 38, 10) is null and 
                    src:coop_3 is not null) THEN
                    'coop_3 contains a non-numeric value : \'' || AS_VARCHAR(src:coop_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_1), '0'), 38, 10) is null and 
                    src:copr_1 is not null) THEN
                    'copr_1 contains a non-numeric value : \'' || AS_VARCHAR(src:copr_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_2), '0'), 38, 10) is null and 
                    src:copr_2 is not null) THEN
                    'copr_2 contains a non-numeric value : \'' || AS_VARCHAR(src:copr_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_3), '0'), 38, 10) is null and 
                    src:copr_3 is not null) THEN
                    'copr_3 contains a non-numeric value : \'' || AS_VARCHAR(src:copr_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crej_ref_compnr), '0'), 38, 10) is null and 
                    src:crej_ref_compnr is not null) THEN
                    'crej_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:crej_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuva), '0'), 38, 10) is null and 
                    src:cuva is not null) THEN
                    'cuva contains a non-numeric value : \'' || AS_VARCHAR(src:cuva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) THEN
                    'cuvc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuvc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:damt), '0'), 38, 10) is null and 
                    src:damt is not null) THEN
                    'damt contains a non-numeric value : \'' || AS_VARCHAR(src:damt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddte), '1900-01-01')) is null and 
                    src:ddte is not null) THEN
                    'ddte contains a non-timestamp value : \'' || AS_VARCHAR(src:ddte) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fire), '0'), 38, 10) is null and 
                    src:fire is not null) THEN
                    'fire contains a non-numeric value : \'' || AS_VARCHAR(src:fire) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ldat), '1900-01-01')) is null and 
                    src:ldat is not null) THEN
                    'ldat contains a non-timestamp value : \'' || AS_VARCHAR(src:ldat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lssn_ref_compnr), '0'), 38, 10) is null and 
                    src:lssn_ref_compnr is not null) THEN
                    'lssn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lssn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpnr_cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:mpnr_cmnf_ref_compnr is not null) THEN
                    'mpnr_cmnf_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mpnr_cmnf_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:onpr), '0'), 38, 10) is null and 
                    src:onpr is not null) THEN
                    'onpr contains a non-numeric value : \'' || AS_VARCHAR(src:onpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_ref_compnr is not null) THEN
                    'orno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap), '0'), 38, 10) is null and 
                    src:qiap is not null) THEN
                    'qiap contains a non-numeric value : \'' || AS_VARCHAR(src:qiap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl), '0'), 38, 10) is null and 
                    src:qidl is not null) THEN
                    'qidl contains a non-numeric value : \'' || AS_VARCHAR(src:qidl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qips), '0'), 38, 10) is null and 
                    src:qips is not null) THEN
                    'qips contains a non-numeric value : \'' || AS_VARCHAR(src:qips) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj), '0'), 38, 10) is null and 
                    src:qirj is not null) THEN
                    'qirj contains a non-numeric value : \'' || AS_VARCHAR(src:qirj) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpap), '0'), 38, 10) is null and 
                    src:qpap is not null) THEN
                    'qpap contains a non-numeric value : \'' || AS_VARCHAR(src:qpap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpdl), '0'), 38, 10) is null and 
                    src:qpdl is not null) THEN
                    'qpdl contains a non-numeric value : \'' || AS_VARCHAR(src:qpdl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpps), '0'), 38, 10) is null and 
                    src:qpps is not null) THEN
                    'qpps contains a non-numeric value : \'' || AS_VARCHAR(src:qpps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qprj), '0'), 38, 10) is null and 
                    src:qprj is not null) THEN
                    'qprj contains a non-numeric value : \'' || AS_VARCHAR(src:qprj) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qual), '0'), 38, 10) is null and 
                    src:qual is not null) THEN
                    'qual contains a non-numeric value : \'' || AS_VARCHAR(src:qual) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rcld), '1900-01-01')) is null and 
                    src:rcld is not null) THEN
                    'rcld contains a non-timestamp value : \'' || AS_VARCHAR(src:rcld) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) THEN
                    'rseq contains a non-numeric value : \'' || AS_VARCHAR(src:rseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsqn), '0'), 38, 10) is null and 
                    src:rsqn is not null) THEN
                    'rsqn contains a non-numeric value : \'' || AS_VARCHAR(src:rsqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:shdt), '1900-01-01')) is null and 
                    src:shdt is not null) THEN
                    'shdt contains a non-timestamp value : \'' || AS_VARCHAR(src:shdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) THEN
                    'sqnb contains a non-numeric value : \'' || AS_VARCHAR(src:sqnb) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) THEN
                    'wght contains a non-numeric value : \'' || AS_VARCHAR(src:wght) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtun_ref_compnr), '0'), 38, 10) is null and 
                    src:wtun_ref_compnr is not null) THEN
                    'wtun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:wtun_ref_compnr) || '\'' WHEN 
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
                                    
                src:orno,
                src:pono,
                src:rsqn,
                src:sqnb,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR406 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrw_ref_compnr), '0'), 38, 10) is null and 
                    src:afrw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld), '0'), 38, 10) is null and 
                    src:amld is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod), '0'), 38, 10) is null and 
                    src:amod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arej), '0'), 38, 10) is null and 
                    src:arej is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgxc), '0'), 38, 10) is null and 
                    src:bgxc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:cmnf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:conf), '0'), 38, 10) is null and 
                    src:conf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coop_1), '0'), 38, 10) is null and 
                    src:coop_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coop_2), '0'), 38, 10) is null and 
                    src:coop_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coop_3), '0'), 38, 10) is null and 
                    src:coop_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_1), '0'), 38, 10) is null and 
                    src:copr_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_2), '0'), 38, 10) is null and 
                    src:copr_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr_3), '0'), 38, 10) is null and 
                    src:copr_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crej_ref_compnr), '0'), 38, 10) is null and 
                    src:crej_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuva), '0'), 38, 10) is null and 
                    src:cuva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:damt), '0'), 38, 10) is null and 
                    src:damt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddte), '1900-01-01')) is null and 
                    src:ddte is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fire), '0'), 38, 10) is null and 
                    src:fire is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ldat), '1900-01-01')) is null and 
                    src:ldat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lssn_ref_compnr), '0'), 38, 10) is null and 
                    src:lssn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpnr_cmnf_ref_compnr), '0'), 38, 10) is null and 
                    src:mpnr_cmnf_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:onpr), '0'), 38, 10) is null and 
                    src:onpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiap), '0'), 38, 10) is null and 
                    src:qiap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidl), '0'), 38, 10) is null and 
                    src:qidl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qips), '0'), 38, 10) is null and 
                    src:qips is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qirj), '0'), 38, 10) is null and 
                    src:qirj is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpap), '0'), 38, 10) is null and 
                    src:qpap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpdl), '0'), 38, 10) is null and 
                    src:qpdl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpps), '0'), 38, 10) is null and 
                    src:qpps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qprj), '0'), 38, 10) is null and 
                    src:qprj is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qual), '0'), 38, 10) is null and 
                    src:qual is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rcld), '1900-01-01')) is null and 
                    src:rcld is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rseq), '0'), 38, 10) is null and 
                    src:rseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsqn), '0'), 38, 10) is null and 
                    src:rsqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:shdt), '1900-01-01')) is null and 
                    src:shdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wght), '0'), 38, 10) is null and 
                    src:wght is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtun_ref_compnr), '0'), 38, 10) is null and 
                    src:wtun_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)
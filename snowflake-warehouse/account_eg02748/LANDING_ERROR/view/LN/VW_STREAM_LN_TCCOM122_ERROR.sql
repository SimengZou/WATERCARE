CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCCOM122_ERROR AS SELECT src, 'LN_TCCOM122' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) THEN
                    'bpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpst), '0'), 38, 10) is null and 
                    src:bpst is not null) THEN
                    'bpst contains a non-numeric value : \'' || AS_VARCHAR(src:bpst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbps_ref_compnr), '0'), 38, 10) is null and 
                    src:cbps_ref_compnr is not null) THEN
                    'cbps_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbps_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbtp_ref_compnr), '0'), 38, 10) is null and 
                    src:cbtp_ref_compnr is not null) THEN
                    'cbtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cbtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccnt_ref_compnr), '0'), 38, 10) is null and 
                    src:ccnt_ref_compnr is not null) THEN
                    'ccnt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccnt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) THEN
                    'ccrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdai), '0'), 38, 10) is null and 
                    src:cdai is not null) THEN
                    'cdai contains a non-numeric value : \'' || AS_VARCHAR(src:cdai) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdap), '0'), 38, 10) is null and 
                    src:cdap is not null) THEN
                    'cdap contains a non-numeric value : \'' || AS_VARCHAR(src:cdap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfsg_rcmp), '0'), 38, 10) is null and 
                    src:cfsg_rcmp is not null) THEN
                    'cfsg_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:cfsg_rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) THEN
                    'clan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crlr), '0'), 38, 10) is null and 
                    src:crlr is not null) THEN
                    'crlr contains a non-numeric value : \'' || AS_VARCHAR(src:crlr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) THEN
                    'cvyn contains a non-numeric value : \'' || AS_VARCHAR(src:cvyn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dial), '0'), 38, 10) is null and 
                    src:dial is not null) THEN
                    'dial contains a non-numeric value : \'' || AS_VARCHAR(src:dial) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dinc), '0'), 38, 10) is null and 
                    src:dinc is not null) THEN
                    'dinc contains a non-numeric value : \'' || AS_VARCHAR(src:dinc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dlcr), '1900-01-01')) is null and 
                    src:dlcr is not null) THEN
                    'dlcr contains a non-timestamp value : \'' || AS_VARCHAR(src:dlcr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eicd), '0'), 38, 10) is null and 
                    src:eicd is not null) THEN
                    'eicd contains a non-numeric value : \'' || AS_VARCHAR(src:eicd) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:endt), '1900-01-01')) is null and 
                    src:endt is not null) THEN
                    'endt contains a non-timestamp value : \'' || AS_VARCHAR(src:endt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etyp), '0'), 38, 10) is null and 
                    src:etyp is not null) THEN
                    'etyp contains a non-numeric value : \'' || AS_VARCHAR(src:etyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) THEN
                    'ifbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ifbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incb), '0'), 38, 10) is null and 
                    src:incb is not null) THEN
                    'incb contains a non-numeric value : \'' || AS_VARCHAR(src:incb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incc), '0'), 38, 10) is null and 
                    src:incc is not null) THEN
                    'incc contains a non-numeric value : \'' || AS_VARCHAR(src:incc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incy), '0'), 38, 10) is null and 
                    src:incy is not null) THEN
                    'incy contains a non-numeric value : \'' || AS_VARCHAR(src:incy) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inma), '0'), 38, 10) is null and 
                    src:inma is not null) THEN
                    'inma contains a non-numeric value : \'' || AS_VARCHAR(src:inma) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issp), '0'), 38, 10) is null and 
                    src:issp is not null) THEN
                    'issp contains a non-numeric value : \'' || AS_VARCHAR(src:issp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) THEN
                    'lmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncin), '0'), 38, 10) is null and 
                    src:ncin is not null) THEN
                    'ncin contains a non-numeric value : \'' || AS_VARCHAR(src:ncin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pana_ref_compnr), '0'), 38, 10) is null and 
                    src:pana_ref_compnr is not null) THEN
                    'pana_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pana_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paya_ref_compnr), '0'), 38, 10) is null and 
                    src:paya_ref_compnr is not null) THEN
                    'paya_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:paya_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_ref_compnr is not null) THEN
                    'ptbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcam), '0'), 38, 10) is null and 
                    src:rcam is not null) THEN
                    'rcam contains a non-numeric value : \'' || AS_VARCHAR(src:rcam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrs_ref_compnr), '0'), 38, 10) is null and 
                    src:rcrs_ref_compnr is not null) THEN
                    'rcrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rcrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpay_ref_compnr), '0'), 38, 10) is null and 
                    src:rpay_ref_compnr is not null) THEN
                    'rpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrsi), '0'), 38, 10) is null and 
                    src:rrsi is not null) THEN
                    'rrsi contains a non-numeric value : \'' || AS_VARCHAR(src:rrsi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) THEN
                    'rtyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rtyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbmt_ref_compnr), '0'), 38, 10) is null and 
                    src:sbmt_ref_compnr is not null) THEN
                    'sbmt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sbmt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spss_ref_compnr), '0'), 38, 10) is null and 
                    src:spss_ref_compnr is not null) THEN
                    'spss_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:spss_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) THEN
                    'stdt contains a non-timestamp value : \'' || AS_VARCHAR(src:stdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) THEN
                    'subc contains a non-numeric value : \'' || AS_VARCHAR(src:subc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usin), '0'), 38, 10) is null and 
                    src:usin is not null) THEN
                    'usin contains a non-numeric value : \'' || AS_VARCHAR(src:usin) || '\'' WHEN 
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
                                    
                src:ifbp,
                src:compnr,
                src:cofc  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCCOM122 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:bpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpst), '0'), 38, 10) is null and 
                    src:bpst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbps_ref_compnr), '0'), 38, 10) is null and 
                    src:cbps_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbtp_ref_compnr), '0'), 38, 10) is null and 
                    src:cbtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccnt_ref_compnr), '0'), 38, 10) is null and 
                    src:ccnt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdai), '0'), 38, 10) is null and 
                    src:cdai is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdap), '0'), 38, 10) is null and 
                    src:cdap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfsg_rcmp), '0'), 38, 10) is null and 
                    src:cfsg_rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crlr), '0'), 38, 10) is null and 
                    src:crlr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvyn), '0'), 38, 10) is null and 
                    src:cvyn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dial), '0'), 38, 10) is null and 
                    src:dial is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dinc), '0'), 38, 10) is null and 
                    src:dinc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dlcr), '1900-01-01')) is null and 
                    src:dlcr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eicd), '0'), 38, 10) is null and 
                    src:eicd is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:endt), '1900-01-01')) is null and 
                    src:endt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etyp), '0'), 38, 10) is null and 
                    src:etyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incb), '0'), 38, 10) is null and 
                    src:incb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incc), '0'), 38, 10) is null and 
                    src:incc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incy), '0'), 38, 10) is null and 
                    src:incy is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inma), '0'), 38, 10) is null and 
                    src:inma is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:issp), '0'), 38, 10) is null and 
                    src:issp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lmdt), '1900-01-01')) is null and 
                    src:lmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncin), '0'), 38, 10) is null and 
                    src:ncin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pana_ref_compnr), '0'), 38, 10) is null and 
                    src:pana_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paya_ref_compnr), '0'), 38, 10) is null and 
                    src:paya_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcam), '0'), 38, 10) is null and 
                    src:rcam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcrs_ref_compnr), '0'), 38, 10) is null and 
                    src:rcrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpay_ref_compnr), '0'), 38, 10) is null and 
                    src:rpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rrsi), '0'), 38, 10) is null and 
                    src:rrsi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtyp_ref_compnr), '0'), 38, 10) is null and 
                    src:rtyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sbmt_ref_compnr), '0'), 38, 10) is null and 
                    src:sbmt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spss_ref_compnr), '0'), 38, 10) is null and 
                    src:spss_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:stdt), '1900-01-01')) is null and 
                    src:stdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usin), '0'), 38, 10) is null and 
                    src:usin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vryn), '0'), 38, 10) is null and 
                    src:vryn is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)
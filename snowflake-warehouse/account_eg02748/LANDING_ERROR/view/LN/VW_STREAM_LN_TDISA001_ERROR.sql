CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDISA001_ERROR AS SELECT src, 'LN_TDISA001' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acci), '0'), 38, 10) is null and 
                    src:acci is not null) THEN
                    'acci contains a non-numeric value : \'' || AS_VARCHAR(src:acci) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acom), '0'), 38, 10) is null and 
                    src:acom is not null) THEN
                    'acom contains a non-numeric value : \'' || AS_VARCHAR(src:acom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actn), '0'), 38, 10) is null and 
                    src:actn is not null) THEN
                    'actn contains a non-numeric value : \'' || AS_VARCHAR(src:actn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrb), '0'), 38, 10) is null and 
                    src:afrb is not null) THEN
                    'afrb contains a non-numeric value : \'' || AS_VARCHAR(src:afrb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alod), '0'), 38, 10) is null and 
                    src:alod is not null) THEN
                    'alod contains a non-numeric value : \'' || AS_VARCHAR(src:alod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:bfbp_ref_compnr is not null) THEN
                    'bfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmgp_ref_compnr), '0'), 38, 10) is null and 
                    src:cmgp_ref_compnr is not null) THEN
                    'cmgp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmgp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgs_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgs_ref_compnr is not null) THEN
                    'cpgs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpgs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cphl), '0'), 38, 10) is null and 
                    src:cphl is not null) THEN
                    'cphl contains a non-numeric value : \'' || AS_VARCHAR(src:cphl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csgs_ref_compnr), '0'), 38, 10) is null and 
                    src:csgs_ref_compnr is not null) THEN
                    'csgs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csgs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cups_ref_compnr), '0'), 38, 10) is null and 
                    src:cups_ref_compnr is not null) THEN
                    'cups_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cups_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqi_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqi_ref_compnr is not null) THEN
                    'cuqi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuqi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqs_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqs_ref_compnr is not null) THEN
                    'cuqs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuqs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_sfsi_ref_compnr), '0'), 38, 10) is null and 
                    src:item_sfsi_ref_compnr is not null) THEN
                    'item_sfsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_sfsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) THEN
                    'lcmp contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) THEN
                    'lcmp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcmp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmsp), '0'), 38, 10) is null and 
                    src:lmsp is not null) THEN
                    'lmsp contains a non-numeric value : \'' || AS_VARCHAR(src:lmsp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltsp), '1900-01-01')) is null and 
                    src:ltsp is not null) THEN
                    'ltsp contains a non-timestamp value : \'' || AS_VARCHAR(src:ltsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mmtl), '0'), 38, 10) is null and 
                    src:mmtl is not null) THEN
                    'mmtl contains a non-numeric value : \'' || AS_VARCHAR(src:mmtl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mmtp), '0'), 38, 10) is null and 
                    src:mmtp is not null) THEN
                    'mmtp contains a non-numeric value : \'' || AS_VARCHAR(src:mmtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mnra), '0'), 38, 10) is null and 
                    src:mnra is not null) THEN
                    'mnra contains a non-numeric value : \'' || AS_VARCHAR(src:mnra) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prir), '0'), 38, 10) is null and 
                    src:prir is not null) THEN
                    'prir contains a non-numeric value : \'' || AS_VARCHAR(src:prir) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) THEN
                    'pris contains a non-numeric value : \'' || AS_VARCHAR(src:pris) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_dtwc), '0'), 38, 10) is null and 
                    src:pris_dtwc is not null) THEN
                    'pris_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:pris_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_lclc), '0'), 38, 10) is null and 
                    src:pris_lclc is not null) THEN
                    'pris_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:pris_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_rfrc), '0'), 38, 10) is null and 
                    src:pris_rfrc is not null) THEN
                    'pris_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:pris_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_rpc1), '0'), 38, 10) is null and 
                    src:pris_rpc1 is not null) THEN
                    'pris_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:pris_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_rpc2), '0'), 38, 10) is null and 
                    src:pris_rpc2 is not null) THEN
                    'pris_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:pris_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidd), '0'), 38, 10) is null and 
                    src:qidd is not null) THEN
                    'qidd contains a non-numeric value : \'' || AS_VARCHAR(src:qidd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qimc), '0'), 38, 10) is null and 
                    src:qimc is not null) THEN
                    'qimc contains a non-numeric value : \'' || AS_VARCHAR(src:qimc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qimo), '0'), 38, 10) is null and 
                    src:qimo is not null) THEN
                    'qimo contains a non-numeric value : \'' || AS_VARCHAR(src:qimo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpdd), '0'), 38, 10) is null and 
                    src:qpdd is not null) THEN
                    'qpdd contains a non-numeric value : \'' || AS_VARCHAR(src:qpdd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpmo), '0'), 38, 10) is null and 
                    src:qpmo is not null) THEN
                    'qpmo contains a non-numeric value : \'' || AS_VARCHAR(src:qpmo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rbgp_ref_compnr), '0'), 38, 10) is null and 
                    src:rbgp_ref_compnr is not null) THEN
                    'rbgp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rbgp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcom), '0'), 38, 10) is null and 
                    src:rcom is not null) THEN
                    'rcom contains a non-numeric value : \'' || AS_VARCHAR(src:rcom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retw), '0'), 38, 10) is null and 
                    src:retw is not null) THEN
                    'retw contains a non-numeric value : \'' || AS_VARCHAR(src:retw) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) THEN
                    'scon contains a non-numeric value : \'' || AS_VARCHAR(src:scon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) THEN
                    'sfbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfsi_ref_compnr), '0'), 38, 10) is null and 
                    src:sfsi_ref_compnr is not null) THEN
                    'sfsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tltp), '0'), 38, 10) is null and 
                    src:tltp is not null) THEN
                    'tltp contains a non-numeric value : \'' || AS_VARCHAR(src:tltp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txts), '0'), 38, 10) is null and 
                    src:txts is not null) THEN
                    'txts contains a non-numeric value : \'' || AS_VARCHAR(src:txts) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txts_ref_compnr), '0'), 38, 10) is null and 
                    src:txts_ref_compnr is not null) THEN
                    'txts_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txts_ref_compnr) || '\'' WHEN 
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
                                    
                src:item,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDISA001 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acci), '0'), 38, 10) is null and 
                    src:acci is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acom), '0'), 38, 10) is null and 
                    src:acom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:actn), '0'), 38, 10) is null and 
                    src:actn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afrb), '0'), 38, 10) is null and 
                    src:afrb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alod), '0'), 38, 10) is null and 
                    src:alod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:bfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmgp_ref_compnr), '0'), 38, 10) is null and 
                    src:cmgp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgs_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cphl), '0'), 38, 10) is null and 
                    src:cphl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csgs_ref_compnr), '0'), 38, 10) is null and 
                    src:csgs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cups_ref_compnr), '0'), 38, 10) is null and 
                    src:cups_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqi_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuqs_ref_compnr), '0'), 38, 10) is null and 
                    src:cuqs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_sfsi_ref_compnr), '0'), 38, 10) is null and 
                    src:item_sfsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp), '0'), 38, 10) is null and 
                    src:lcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcmp_ref_compnr), '0'), 38, 10) is null and 
                    src:lcmp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmsp), '0'), 38, 10) is null and 
                    src:lmsp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltsp), '1900-01-01')) is null and 
                    src:ltsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mmtl), '0'), 38, 10) is null and 
                    src:mmtl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mmtp), '0'), 38, 10) is null and 
                    src:mmtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mnra), '0'), 38, 10) is null and 
                    src:mnra is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prir), '0'), 38, 10) is null and 
                    src:prir is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris), '0'), 38, 10) is null and 
                    src:pris is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_dtwc), '0'), 38, 10) is null and 
                    src:pris_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_lclc), '0'), 38, 10) is null and 
                    src:pris_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_rfrc), '0'), 38, 10) is null and 
                    src:pris_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_rpc1), '0'), 38, 10) is null and 
                    src:pris_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pris_rpc2), '0'), 38, 10) is null and 
                    src:pris_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qidd), '0'), 38, 10) is null and 
                    src:qidd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qimc), '0'), 38, 10) is null and 
                    src:qimc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qimo), '0'), 38, 10) is null and 
                    src:qimo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpdd), '0'), 38, 10) is null and 
                    src:qpdd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpmo), '0'), 38, 10) is null and 
                    src:qpmo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rbgp_ref_compnr), '0'), 38, 10) is null and 
                    src:rbgp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcom), '0'), 38, 10) is null and 
                    src:rcom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retw), '0'), 38, 10) is null and 
                    src:retw is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfbp_ref_compnr), '0'), 38, 10) is null and 
                    src:sfbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfsi_ref_compnr), '0'), 38, 10) is null and 
                    src:sfsi_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tltp), '0'), 38, 10) is null and 
                    src:tltp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txts), '0'), 38, 10) is null and 
                    src:txts is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txts_ref_compnr), '0'), 38, 10) is null and 
                    src:txts_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:umsp), '0'), 38, 10) is null and 
                    src:umsp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)
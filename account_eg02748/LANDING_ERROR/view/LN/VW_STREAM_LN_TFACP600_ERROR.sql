CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFACP600_ERROR AS SELECT src, 'LN_TFACP600' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) THEN
                    'amnt contains a non-numeric value : \'' || AS_VARCHAR(src:amnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtl), '0'), 38, 10) is null and 
                    src:amtl is not null) THEN
                    'amtl contains a non-numeric value : \'' || AS_VARCHAR(src:amtl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddat), '1900-01-01')) is null and 
                    src:ddat is not null) THEN
                    'ddat contains a non-timestamp value : \'' || AS_VARCHAR(src:ddat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gcom), '0'), 38, 10) is null and 
                    src:gcom is not null) THEN
                    'gcom contains a non-numeric value : \'' || AS_VARCHAR(src:gcom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gdoc), '0'), 38, 10) is null and 
                    src:gdoc is not null) THEN
                    'gdoc contains a non-numeric value : \'' || AS_VARCHAR(src:gdoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glin), '0'), 38, 10) is null and 
                    src:glin is not null) THEN
                    'glin contains a non-numeric value : \'' || AS_VARCHAR(src:glin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipco), '0'), 38, 10) is null and 
                    src:ipco is not null) THEN
                    'ipco contains a non-numeric value : \'' || AS_VARCHAR(src:ipco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipdo), '0'), 38, 10) is null and 
                    src:ipdo is not null) THEN
                    'ipdo contains a non-numeric value : \'' || AS_VARCHAR(src:ipdo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipli), '0'), 38, 10) is null and 
                    src:ipli is not null) THEN
                    'ipli contains a non-numeric value : \'' || AS_VARCHAR(src:ipli) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:payd), '0'), 38, 10) is null and 
                    src:payd is not null) THEN
                    'payd contains a non-numeric value : \'' || AS_VARCHAR(src:payd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:payl), '0'), 38, 10) is null and 
                    src:payl is not null) THEN
                    'payl contains a non-numeric value : \'' || AS_VARCHAR(src:payl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbcp), '0'), 38, 10) is null and 
                    src:pbcp is not null) THEN
                    'pbcp contains a non-numeric value : \'' || AS_VARCHAR(src:pbcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbtn), '0'), 38, 10) is null and 
                    src:pbtn is not null) THEN
                    'pbtn contains a non-numeric value : \'' || AS_VARCHAR(src:pbtn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcom), '0'), 38, 10) is null and 
                    src:pcom is not null) THEN
                    'pcom contains a non-numeric value : \'' || AS_VARCHAR(src:pcom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_ref_compnr is not null) THEN
                    'ptbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) THEN
                    'sdat contains a non-timestamp value : \'' || AS_VARCHAR(src:sdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:step), '0'), 38, 10) is null and 
                    src:step is not null) THEN
                    'step contains a non-numeric value : \'' || AS_VARCHAR(src:step) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpay), '0'), 38, 10) is null and 
                    src:tpay is not null) THEN
                    'tpay contains a non-numeric value : \'' || AS_VARCHAR(src:tpay) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbp_ref_compnr), '0'), 38, 10) is null and 
                    src:tpbp_ref_compnr is not null) THEN
                    'tpbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tpbp_ref_compnr) || '\'' WHEN 
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
                                    
                src:payd,
                src:payt,
                src:compnr,
                src:payl,
                src:seqn,
                src:pcom  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFACP600 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amtl), '0'), 38, 10) is null and 
                    src:amtl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ddat), '1900-01-01')) is null and 
                    src:ddat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gcom), '0'), 38, 10) is null and 
                    src:gcom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gdoc), '0'), 38, 10) is null and 
                    src:gdoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:glin), '0'), 38, 10) is null and 
                    src:glin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipco), '0'), 38, 10) is null and 
                    src:ipco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipdo), '0'), 38, 10) is null and 
                    src:ipdo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ipli), '0'), 38, 10) is null and 
                    src:ipli is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:payd), '0'), 38, 10) is null and 
                    src:payd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:payl), '0'), 38, 10) is null and 
                    src:payl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbcp), '0'), 38, 10) is null and 
                    src:pbcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pbtn), '0'), 38, 10) is null and 
                    src:pbtn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcom), '0'), 38, 10) is null and 
                    src:pcom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sdat), '1900-01-01')) is null and 
                    src:sdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:step), '0'), 38, 10) is null and 
                    src:step is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpay), '0'), 38, 10) is null and 
                    src:tpay is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpbp_ref_compnr), '0'), 38, 10) is null and 
                    src:tpbp_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)
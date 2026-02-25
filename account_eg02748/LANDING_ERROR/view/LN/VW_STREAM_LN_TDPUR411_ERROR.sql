CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDPUR411_ERROR AS SELECT src, 'LN_TDPUR411' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) THEN
                    'atse_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:atse_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atsg_ref_compnr), '0'), 38, 10) is null and 
                    src:atsg_ref_compnr is not null) THEN
                    'atsg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:atsg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is null and 
                    src:citg_ref_compnr is not null) THEN
                    'citg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:citg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcl_ref_compnr is not null) THEN
                    'cpcl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpcl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgp_ref_compnr is not null) THEN
                    'cpgp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpgp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpln_ref_compnr), '0'), 38, 10) is null and 
                    src:cpln_ref_compnr is not null) THEN
                    'cpln_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpln_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csgp_ref_compnr), '0'), 38, 10) is null and 
                    src:csgp_ref_compnr is not null) THEN
                    'csgp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csgp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iwgt), '0'), 38, 10) is null and 
                    src:iwgt is not null) THEN
                    'iwgt contains a non-numeric value : \'' || AS_VARCHAR(src:iwgt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iwun_ref_compnr), '0'), 38, 10) is null and 
                    src:iwun_ref_compnr is not null) THEN
                    'iwun_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:iwun_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kitm), '0'), 38, 10) is null and 
                    src:kitm is not null) THEN
                    'kitm contains a non-numeric value : \'' || AS_VARCHAR(src:kitm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_pono_sqnb_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_pono_sqnb_ref_compnr is not null) THEN
                    'orno_pono_sqnb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orno_pono_sqnb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_ref_compnr is not null) THEN
                    'orno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:orno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgmd), '0'), 38, 10) is null and 
                    src:pgmd is not null) THEN
                    'pgmd contains a non-numeric value : \'' || AS_VARCHAR(src:pgmd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prtp_ref_compnr), '0'), 38, 10) is null and 
                    src:prtp_ref_compnr is not null) THEN
                    'prtp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prtp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) THEN
                    'sqnb contains a non-numeric value : \'' || AS_VARCHAR(src:sqnb) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
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
                src:sqnb,
                src:orno,
                src:pono  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR411 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atse_ref_compnr), '0'), 38, 10) is null and 
                    src:atse_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:atsg_ref_compnr), '0'), 38, 10) is null and 
                    src:atsg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:citg_ref_compnr), '0'), 38, 10) is null and 
                    src:citg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpcl_ref_compnr), '0'), 38, 10) is null and 
                    src:cpcl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpgp_ref_compnr), '0'), 38, 10) is null and 
                    src:cpgp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpln_ref_compnr), '0'), 38, 10) is null and 
                    src:cpln_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csgp_ref_compnr), '0'), 38, 10) is null and 
                    src:csgp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iwgt), '0'), 38, 10) is null and 
                    src:iwgt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iwun_ref_compnr), '0'), 38, 10) is null and 
                    src:iwun_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kitm), '0'), 38, 10) is null and 
                    src:kitm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_pono_sqnb_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_pono_sqnb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orno_ref_compnr), '0'), 38, 10) is null and 
                    src:orno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pgmd), '0'), 38, 10) is null and 
                    src:pgmd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prtp_ref_compnr), '0'), 38, 10) is null and 
                    src:prtp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)
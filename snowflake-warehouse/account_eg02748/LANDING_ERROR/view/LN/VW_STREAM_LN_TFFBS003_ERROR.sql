CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFFBS003_ERROR AS SELECT src, 'LN_TFFBS003' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ad10), '0'), 38, 10) is null and 
                    src:ad10 is not null) THEN
                    'ad10 contains a non-numeric value : \'' || AS_VARCHAR(src:ad10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ad11), '0'), 38, 10) is null and 
                    src:ad11 is not null) THEN
                    'ad11 contains a non-numeric value : \'' || AS_VARCHAR(src:ad11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ad12), '0'), 38, 10) is null and 
                    src:ad12 is not null) THEN
                    'ad12 contains a non-numeric value : \'' || AS_VARCHAR(src:ad12) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt1), '0'), 38, 10) is null and 
                    src:adt1 is not null) THEN
                    'adt1 contains a non-numeric value : \'' || AS_VARCHAR(src:adt1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt2), '0'), 38, 10) is null and 
                    src:adt2 is not null) THEN
                    'adt2 contains a non-numeric value : \'' || AS_VARCHAR(src:adt2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt3), '0'), 38, 10) is null and 
                    src:adt3 is not null) THEN
                    'adt3 contains a non-numeric value : \'' || AS_VARCHAR(src:adt3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt4), '0'), 38, 10) is null and 
                    src:adt4 is not null) THEN
                    'adt4 contains a non-numeric value : \'' || AS_VARCHAR(src:adt4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt5), '0'), 38, 10) is null and 
                    src:adt5 is not null) THEN
                    'adt5 contains a non-numeric value : \'' || AS_VARCHAR(src:adt5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt6), '0'), 38, 10) is null and 
                    src:adt6 is not null) THEN
                    'adt6 contains a non-numeric value : \'' || AS_VARCHAR(src:adt6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt7), '0'), 38, 10) is null and 
                    src:adt7 is not null) THEN
                    'adt7 contains a non-numeric value : \'' || AS_VARCHAR(src:adt7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt8), '0'), 38, 10) is null and 
                    src:adt8 is not null) THEN
                    'adt8 contains a non-numeric value : \'' || AS_VARCHAR(src:adt8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt9), '0'), 38, 10) is null and 
                    src:adt9 is not null) THEN
                    'adt9 contains a non-numeric value : \'' || AS_VARCHAR(src:adt9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aqu1), '0'), 38, 10) is null and 
                    src:aqu1 is not null) THEN
                    'aqu1 contains a non-numeric value : \'' || AS_VARCHAR(src:aqu1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aqu2), '0'), 38, 10) is null and 
                    src:aqu2 is not null) THEN
                    'aqu2 contains a non-numeric value : \'' || AS_VARCHAR(src:aqu2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:budm), '0'), 38, 10) is null and 
                    src:budm is not null) THEN
                    'budm contains a non-numeric value : \'' || AS_VARCHAR(src:budm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:llac), '0'), 38, 10) is null and 
                    src:llac is not null) THEN
                    'llac contains a non-numeric value : \'' || AS_VARCHAR(src:llac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nbpr), '0'), 38, 10) is null and 
                    src:nbpr is not null) THEN
                    'nbpr contains a non-numeric value : \'' || AS_VARCHAR(src:nbpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdbu), '0'), 38, 10) is null and 
                    src:sdbu is not null) THEN
                    'sdbu contains a non-numeric value : \'' || AS_VARCHAR(src:sdbu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
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
                                    
                src:budg,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFBS003 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ad10), '0'), 38, 10) is null and 
                    src:ad10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ad11), '0'), 38, 10) is null and 
                    src:ad11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ad12), '0'), 38, 10) is null and 
                    src:ad12 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt1), '0'), 38, 10) is null and 
                    src:adt1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt2), '0'), 38, 10) is null and 
                    src:adt2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt3), '0'), 38, 10) is null and 
                    src:adt3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt4), '0'), 38, 10) is null and 
                    src:adt4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt5), '0'), 38, 10) is null and 
                    src:adt5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt6), '0'), 38, 10) is null and 
                    src:adt6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt7), '0'), 38, 10) is null and 
                    src:adt7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt8), '0'), 38, 10) is null and 
                    src:adt8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adt9), '0'), 38, 10) is null and 
                    src:adt9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aqu1), '0'), 38, 10) is null and 
                    src:aqu1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aqu2), '0'), 38, 10) is null and 
                    src:aqu2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:budm), '0'), 38, 10) is null and 
                    src:budm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:llac), '0'), 38, 10) is null and 
                    src:llac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nbpr), '0'), 38, 10) is null and 
                    src:nbpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdbu), '0'), 38, 10) is null and 
                    src:sdbu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)
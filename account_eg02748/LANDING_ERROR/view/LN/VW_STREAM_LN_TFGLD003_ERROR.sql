CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFGLD003_ERROR AS SELECT src, 'LN_TFGLD003' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bcmp), '0'), 38, 10) is null and 
                    src:bcmp is not null) THEN
                    'bcmp contains a non-numeric value : \'' || AS_VARCHAR(src:bcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfst), '0'), 38, 10) is null and 
                    src:cfst is not null) THEN
                    'cfst contains a non-numeric value : \'' || AS_VARCHAR(src:cfst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcfi), '0'), 38, 10) is null and 
                    src:dcfi is not null) THEN
                    'dcfi contains a non-numeric value : \'' || AS_VARCHAR(src:dcfi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim1), '0'), 38, 10) is null and 
                    src:dim1 is not null) THEN
                    'dim1 contains a non-numeric value : \'' || AS_VARCHAR(src:dim1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim2), '0'), 38, 10) is null and 
                    src:dim2 is not null) THEN
                    'dim2 contains a non-numeric value : \'' || AS_VARCHAR(src:dim2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim3), '0'), 38, 10) is null and 
                    src:dim3 is not null) THEN
                    'dim3 contains a non-numeric value : \'' || AS_VARCHAR(src:dim3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim4), '0'), 38, 10) is null and 
                    src:dim4 is not null) THEN
                    'dim4 contains a non-numeric value : \'' || AS_VARCHAR(src:dim4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim5), '0'), 38, 10) is null and 
                    src:dim5 is not null) THEN
                    'dim5 contains a non-numeric value : \'' || AS_VARCHAR(src:dim5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim6), '0'), 38, 10) is null and 
                    src:dim6 is not null) THEN
                    'dim6 contains a non-numeric value : \'' || AS_VARCHAR(src:dim6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim7), '0'), 38, 10) is null and 
                    src:dim7 is not null) THEN
                    'dim7 contains a non-numeric value : \'' || AS_VARCHAR(src:dim7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim8), '0'), 38, 10) is null and 
                    src:dim8 is not null) THEN
                    'dim8 contains a non-numeric value : \'' || AS_VARCHAR(src:dim8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim9), '0'), 38, 10) is null and 
                    src:dim9 is not null) THEN
                    'dim9 contains a non-numeric value : \'' || AS_VARCHAR(src:dim9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dm10), '0'), 38, 10) is null and 
                    src:dm10 is not null) THEN
                    'dm10 contains a non-numeric value : \'' || AS_VARCHAR(src:dm10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dm11), '0'), 38, 10) is null and 
                    src:dm11 is not null) THEN
                    'dm11 contains a non-numeric value : \'' || AS_VARCHAR(src:dm11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dm12), '0'), 38, 10) is null and 
                    src:dm12 is not null) THEN
                    'dm12 contains a non-numeric value : \'' || AS_VARCHAR(src:dm12) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gmbi), '0'), 38, 10) is null and 
                    src:gmbi is not null) THEN
                    'gmbi contains a non-numeric value : \'' || AS_VARCHAR(src:gmbi) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) THEN
                    'indt contains a non-timestamp value : \'' || AS_VARCHAR(src:indt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nfpr), '0'), 38, 10) is null and 
                    src:nfpr is not null) THEN
                    'nfpr contains a non-numeric value : \'' || AS_VARCHAR(src:nfpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrpr), '0'), 38, 10) is null and 
                    src:nrpr is not null) THEN
                    'nrpr contains a non-numeric value : \'' || AS_VARCHAR(src:nrpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nvpr), '0'), 38, 10) is null and 
                    src:nvpr is not null) THEN
                    'nvpr contains a non-numeric value : \'' || AS_VARCHAR(src:nvpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:papp), '0'), 38, 10) is null and 
                    src:papp is not null) THEN
                    'papp contains a non-numeric value : \'' || AS_VARCHAR(src:papp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psbk), '0'), 38, 10) is null and 
                    src:psbk is not null) THEN
                    'psbk contains a non-numeric value : \'' || AS_VARCHAR(src:psbk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psic), '0'), 38, 10) is null and 
                    src:psic is not null) THEN
                    'psic contains a non-numeric value : \'' || AS_VARCHAR(src:psic) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pupt), '0'), 38, 10) is null and 
                    src:pupt is not null) THEN
                    'pupt contains a non-numeric value : \'' || AS_VARCHAR(src:pupt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rper), '0'), 38, 10) is null and 
                    src:rper is not null) THEN
                    'rper contains a non-numeric value : \'' || AS_VARCHAR(src:rper) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdat), '0'), 38, 10) is null and 
                    src:sdat is not null) THEN
                    'sdat contains a non-numeric value : \'' || AS_VARCHAR(src:sdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sgmr), '0'), 38, 10) is null and 
                    src:sgmr is not null) THEN
                    'sgmr contains a non-numeric value : \'' || AS_VARCHAR(src:sgmr) || '\'' WHEN 
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
                                    
                src:indt,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD003 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bcmp), '0'), 38, 10) is null and 
                    src:bcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfst), '0'), 38, 10) is null and 
                    src:cfst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcfi), '0'), 38, 10) is null and 
                    src:dcfi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim1), '0'), 38, 10) is null and 
                    src:dim1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim2), '0'), 38, 10) is null and 
                    src:dim2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim3), '0'), 38, 10) is null and 
                    src:dim3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim4), '0'), 38, 10) is null and 
                    src:dim4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim5), '0'), 38, 10) is null and 
                    src:dim5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim6), '0'), 38, 10) is null and 
                    src:dim6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim7), '0'), 38, 10) is null and 
                    src:dim7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim8), '0'), 38, 10) is null and 
                    src:dim8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dim9), '0'), 38, 10) is null and 
                    src:dim9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dm10), '0'), 38, 10) is null and 
                    src:dm10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dm11), '0'), 38, 10) is null and 
                    src:dm11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dm12), '0'), 38, 10) is null and 
                    src:dm12 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gmbi), '0'), 38, 10) is null and 
                    src:gmbi is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nfpr), '0'), 38, 10) is null and 
                    src:nfpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nrpr), '0'), 38, 10) is null and 
                    src:nrpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nvpr), '0'), 38, 10) is null and 
                    src:nvpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:papp), '0'), 38, 10) is null and 
                    src:papp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psbk), '0'), 38, 10) is null and 
                    src:psbk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psic), '0'), 38, 10) is null and 
                    src:psic is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pupt), '0'), 38, 10) is null and 
                    src:pupt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rper), '0'), 38, 10) is null and 
                    src:rper is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdat), '0'), 38, 10) is null and 
                    src:sdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sgmr), '0'), 38, 10) is null and 
                    src:sgmr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)
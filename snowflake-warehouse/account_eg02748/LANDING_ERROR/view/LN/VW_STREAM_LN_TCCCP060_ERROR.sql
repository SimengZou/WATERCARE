CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCCCP060_ERROR AS SELECT src, 'LN_TCCCP060' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtyp), '0'), 38, 10) is null and 
                    src:dtyp is not null) THEN
                    'dtyp contains a non-numeric value : \'' || AS_VARCHAR(src:dtyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dura), '0'), 38, 10) is null and 
                    src:dura is not null) THEN
                    'dura contains a non-numeric value : \'' || AS_VARCHAR(src:dura) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gchk), '0'), 38, 10) is null and 
                    src:gchk is not null) THEN
                    'gchk contains a non-numeric value : \'' || AS_VARCHAR(src:gchk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mchk), '0'), 38, 10) is null and 
                    src:mchk is not null) THEN
                    'mchk contains a non-numeric value : \'' || AS_VARCHAR(src:mchk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sttm), '0'), 38, 10) is null and 
                    src:sttm is not null) THEN
                    'sttm contains a non-numeric value : \'' || AS_VARCHAR(src:sttm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ychk), '0'), 38, 10) is null and 
                    src:ychk is not null) THEN
                    'ychk contains a non-numeric value : \'' || AS_VARCHAR(src:ychk) || '\'' WHEN 
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
                src:cpdt  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCCCP060 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtyp), '0'), 38, 10) is null and 
                    src:dtyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dura), '0'), 38, 10) is null and 
                    src:dura is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gchk), '0'), 38, 10) is null and 
                    src:gchk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mchk), '0'), 38, 10) is null and 
                    src:mchk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sttm), '0'), 38, 10) is null and 
                    src:sttm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ychk), '0'), 38, 10) is null and 
                    src:ychk is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)
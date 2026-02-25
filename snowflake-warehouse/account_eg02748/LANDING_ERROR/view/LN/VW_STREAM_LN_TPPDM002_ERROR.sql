CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPDM002_ERROR AS SELECT src, 'LN_TPPDM002' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maps), '0'), 38, 10) is null and 
                    src:maps is not null) THEN
                    'maps contains a non-numeric value : \'' || AS_VARCHAR(src:maps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpss), '0'), 38, 10) is null and 
                    src:mpss is not null) THEN
                    'mpss contains a non-numeric value : \'' || AS_VARCHAR(src:mpss) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpvw), '0'), 38, 10) is null and 
                    src:mpvw is not null) THEN
                    'mpvw contains a non-numeric value : \'' || AS_VARCHAR(src:mpvw) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sses), '0'), 38, 10) is null and 
                    src:sses is not null) THEN
                    'sses contains a non-numeric value : \'' || AS_VARCHAR(src:sses) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssvw), '0'), 38, 10) is null and 
                    src:ssvw is not null) THEN
                    'ssvw contains a non-numeric value : \'' || AS_VARCHAR(src:ssvw) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprf), '0'), 38, 10) is null and 
                    src:tprf is not null) THEN
                    'tprf contains a non-numeric value : \'' || AS_VARCHAR(src:tprf) || '\'' WHEN 
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
                                    
                src:maps,
                src:loco,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM002 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:maps), '0'), 38, 10) is null and 
                    src:maps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpss), '0'), 38, 10) is null and 
                    src:mpss is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpvw), '0'), 38, 10) is null and 
                    src:mpvw is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sses), '0'), 38, 10) is null and 
                    src:sses is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssvw), '0'), 38, 10) is null and 
                    src:ssvw is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprf), '0'), 38, 10) is null and 
                    src:tprf is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)
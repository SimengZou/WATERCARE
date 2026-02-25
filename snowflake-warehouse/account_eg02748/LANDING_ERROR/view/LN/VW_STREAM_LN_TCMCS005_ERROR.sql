CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCMCS005_ERROR AS SELECT src, 'LN_TCMCS005' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appr), '0'), 38, 10) is null and 
                    src:appr is not null) THEN
                    'appr contains a non-numeric value : \'' || AS_VARCHAR(src:appr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bilb), '0'), 38, 10) is null and 
                    src:bilb is not null) THEN
                    'bilb contains a non-numeric value : \'' || AS_VARCHAR(src:bilb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:drpi), '0'), 38, 10) is null and 
                    src:drpi is not null) THEN
                    'drpi contains a non-numeric value : \'' || AS_VARCHAR(src:drpi) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) THEN
                    'efdt contains a non-timestamp value : \'' || AS_VARCHAR(src:efdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etrt), '0'), 38, 10) is null and 
                    src:etrt is not null) THEN
                    'etrt contains a non-numeric value : \'' || AS_VARCHAR(src:etrt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) THEN
                    'exdt contains a non-timestamp value : \'' || AS_VARCHAR(src:exdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rstp), '0'), 38, 10) is null and 
                    src:rstp is not null) THEN
                    'rstp contains a non-numeric value : \'' || AS_VARCHAR(src:rstp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
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
                                    
                src:cdis,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCMCS005 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appr), '0'), 38, 10) is null and 
                    src:appr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bilb), '0'), 38, 10) is null and 
                    src:bilb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:drpi), '0'), 38, 10) is null and 
                    src:drpi is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etrt), '0'), 38, 10) is null and 
                    src:etrt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rstp), '0'), 38, 10) is null and 
                    src:rstp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)
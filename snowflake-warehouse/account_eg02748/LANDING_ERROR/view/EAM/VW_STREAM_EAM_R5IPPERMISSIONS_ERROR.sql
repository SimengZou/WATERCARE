CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5IPPERMISSIONS_ERROR AS SELECT src, 'EAM_R5IPPERMISSIONS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IPP_CODE), '0'), 38, 10) is null and 
                    src:IPP_CODE is not null) THEN
                    'IPP_CODE contains a non-numeric value : \'' || AS_VARCHAR(src:IPP_CODE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IPP_FUNCTION), '0'), 38, 10) is null and 
                    src:IPP_FUNCTION is not null) THEN
                    'IPP_FUNCTION contains a non-numeric value : \'' || AS_VARCHAR(src:IPP_FUNCTION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IPP_LASTSAVED), '1900-01-01')) is null and 
                    src:IPP_LASTSAVED is not null) THEN
                    'IPP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:IPP_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IPP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:IPP_UPDATECOUNT is not null) THEN
                    'IPP_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:IPP_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IPP_LASTSAVED), '1900-01-01')) is null and 
                src:IPP_LASTSAVED is not null) THEN
                'IPP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:IPP_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:IPP_CODE  ORDER BY 
            src:IPP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5IPPERMISSIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IPP_CODE), '0'), 38, 10) is null and 
                    src:IPP_CODE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IPP_FUNCTION), '0'), 38, 10) is null and 
                    src:IPP_FUNCTION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IPP_LASTSAVED), '1900-01-01')) is null and 
                    src:IPP_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IPP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:IPP_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IPP_LASTSAVED), '1900-01-01')) is null and 
                src:IPP_LASTSAVED is not null)
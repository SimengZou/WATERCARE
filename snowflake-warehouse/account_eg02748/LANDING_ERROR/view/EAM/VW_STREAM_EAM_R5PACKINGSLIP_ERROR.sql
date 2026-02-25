CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5PACKINGSLIP_ERROR AS SELECT src, 'EAM_R5PACKINGSLIP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PSL_DELQTY), '0'), 38, 10) is null and 
                    src:PSL_DELQTY is not null) THEN
                    'PSL_DELQTY contains a non-numeric value : \'' || AS_VARCHAR(src:PSL_DELQTY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PSL_LASTSAVED), '1900-01-01')) is null and 
                    src:PSL_LASTSAVED is not null) THEN
                    'PSL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PSL_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PSL_LINE), '0'), 38, 10) is null and 
                    src:PSL_LINE is not null) THEN
                    'PSL_LINE contains a non-numeric value : \'' || AS_VARCHAR(src:PSL_LINE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PSL_MANLOTEXP), '1900-01-01')) is null and 
                    src:PSL_MANLOTEXP is not null) THEN
                    'PSL_MANLOTEXP contains a non-timestamp value : \'' || AS_VARCHAR(src:PSL_MANLOTEXP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PSL_MULTIPLY), '0'), 38, 10) is null and 
                    src:PSL_MULTIPLY is not null) THEN
                    'PSL_MULTIPLY contains a non-numeric value : \'' || AS_VARCHAR(src:PSL_MULTIPLY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PSL_ORDLINE), '0'), 38, 10) is null and 
                    src:PSL_ORDLINE is not null) THEN
                    'PSL_ORDLINE contains a non-numeric value : \'' || AS_VARCHAR(src:PSL_ORDLINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PSL_RECVQTY), '0'), 38, 10) is null and 
                    src:PSL_RECVQTY is not null) THEN
                    'PSL_RECVQTY contains a non-numeric value : \'' || AS_VARCHAR(src:PSL_RECVQTY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PSL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PSL_UPDATECOUNT is not null) THEN
                    'PSL_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PSL_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PSL_LASTSAVED), '1900-01-01')) is null and 
                src:PSL_LASTSAVED is not null) THEN
                'PSL_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PSL_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PSL_DOCK,
                src:PSL_LINE  ORDER BY 
            src:PSL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PACKINGSLIP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PSL_DELQTY), '0'), 38, 10) is null and 
                    src:PSL_DELQTY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PSL_LASTSAVED), '1900-01-01')) is null and 
                    src:PSL_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PSL_LINE), '0'), 38, 10) is null and 
                    src:PSL_LINE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PSL_MANLOTEXP), '1900-01-01')) is null and 
                    src:PSL_MANLOTEXP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PSL_MULTIPLY), '0'), 38, 10) is null and 
                    src:PSL_MULTIPLY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PSL_ORDLINE), '0'), 38, 10) is null and 
                    src:PSL_ORDLINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PSL_RECVQTY), '0'), 38, 10) is null and 
                    src:PSL_RECVQTY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PSL_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PSL_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PSL_LASTSAVED), '1900-01-01')) is null and 
                src:PSL_LASTSAVED is not null)
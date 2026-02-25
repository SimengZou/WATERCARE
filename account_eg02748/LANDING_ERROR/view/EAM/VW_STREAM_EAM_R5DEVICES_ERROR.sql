CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5DEVICES_ERROR AS SELECT src, 'EAM_R5DEVICES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DEV_LASTLOGINDATE), '1900-01-01')) is null and 
                    src:DEV_LASTLOGINDATE is not null) THEN
                    'DEV_LASTLOGINDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DEV_LASTLOGINDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DEV_LASTSAVED), '1900-01-01')) is null and 
                    src:DEV_LASTSAVED is not null) THEN
                    'DEV_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DEV_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEV_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DEV_UPDATECOUNT is not null) THEN
                    'DEV_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DEV_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DEV_UPDATED), '1900-01-01')) is null and 
                    src:DEV_UPDATED is not null) THEN
                    'DEV_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:DEV_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DEV_LASTSAVED), '1900-01-01')) is null and 
                src:DEV_LASTSAVED is not null) THEN
                'DEV_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:DEV_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:DEV_CODE  ORDER BY 
            src:DEV_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DEVICES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DEV_LASTLOGINDATE), '1900-01-01')) is null and 
                    src:DEV_LASTLOGINDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DEV_LASTSAVED), '1900-01-01')) is null and 
                    src:DEV_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEV_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:DEV_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DEV_UPDATED), '1900-01-01')) is null and 
                    src:DEV_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DEV_LASTSAVED), '1900-01-01')) is null and 
                src:DEV_LASTSAVED is not null)
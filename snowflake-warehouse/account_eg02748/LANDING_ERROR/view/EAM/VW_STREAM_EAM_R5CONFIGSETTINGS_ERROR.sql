CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5CONFIGSETTINGS_ERROR AS SELECT src, 'EAM_R5CONFIGSETTINGS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CFS_CONFIG), '0'), 38, 10) is null and 
                    src:CFS_CONFIG is not null) THEN
                    'CFS_CONFIG contains a non-numeric value : \'' || AS_VARCHAR(src:CFS_CONFIG) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CFS_CREATED), '1900-01-01')) is null and 
                    src:CFS_CREATED is not null) THEN
                    'CFS_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:CFS_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CFS_LASTSAVED), '1900-01-01')) is null and 
                    src:CFS_LASTSAVED is not null) THEN
                    'CFS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CFS_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CFS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CFS_UPDATECOUNT is not null) THEN
                    'CFS_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:CFS_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CFS_UPDATED), '1900-01-01')) is null and 
                    src:CFS_UPDATED is not null) THEN
                    'CFS_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:CFS_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CFS_LASTSAVED), '1900-01-01')) is null and 
                src:CFS_LASTSAVED is not null) THEN
                'CFS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CFS_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:CFS_CODE,
                src:CFS_COMPTYPE,
                src:CFS_CONFIG,
                src:CFS_DESK,
                src:CFS_GROUP,
                src:CFS_USER  ORDER BY 
            src:CFS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CONFIGSETTINGS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CFS_CONFIG), '0'), 38, 10) is null and 
                    src:CFS_CONFIG is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CFS_CREATED), '1900-01-01')) is null and 
                    src:CFS_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CFS_LASTSAVED), '1900-01-01')) is null and 
                    src:CFS_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CFS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:CFS_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CFS_UPDATED), '1900-01-01')) is null and 
                    src:CFS_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CFS_LASTSAVED), '1900-01-01')) is null and 
                src:CFS_LASTSAVED is not null)
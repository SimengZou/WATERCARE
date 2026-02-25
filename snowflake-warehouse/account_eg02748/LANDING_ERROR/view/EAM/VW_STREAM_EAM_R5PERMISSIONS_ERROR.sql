CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5PERMISSIONS_ERROR AS SELECT src, 'EAM_R5PERMISSIONS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRM_CREATED), '1900-01-01')) is null and 
                    src:PRM_CREATED is not null) THEN
                    'PRM_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PRM_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRM_LASTSAVED), '1900-01-01')) is null and 
                    src:PRM_LASTSAVED is not null) THEN
                    'PRM_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PRM_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRM_SECURITYDDSPYID), '0'), 38, 10) is null and 
                    src:PRM_SECURITYDDSPYID is not null) THEN
                    'PRM_SECURITYDDSPYID contains a non-numeric value : \'' || AS_VARCHAR(src:PRM_SECURITYDDSPYID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRM_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PRM_UPDATECOUNT is not null) THEN
                    'PRM_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PRM_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRM_UPDATED), '1900-01-01')) is null and 
                    src:PRM_UPDATED is not null) THEN
                    'PRM_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PRM_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRM_LASTSAVED), '1900-01-01')) is null and 
                src:PRM_LASTSAVED is not null) THEN
                'PRM_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PRM_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PRM_FUNCTION,
                src:PRM_GROUP  ORDER BY 
            src:PRM_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PERMISSIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRM_CREATED), '1900-01-01')) is null and 
                    src:PRM_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRM_LASTSAVED), '1900-01-01')) is null and 
                    src:PRM_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRM_SECURITYDDSPYID), '0'), 38, 10) is null and 
                    src:PRM_SECURITYDDSPYID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRM_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PRM_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRM_UPDATED), '1900-01-01')) is null and 
                    src:PRM_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PRM_LASTSAVED), '1900-01-01')) is null and 
                src:PRM_LASTSAVED is not null)
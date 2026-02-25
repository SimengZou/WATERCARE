CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5MOBILEDBSYNC_ERROR AS SELECT src, 'EAM_R5MOBILEDBSYNC' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MDS_DBLASTUPDATEDDATE), '1900-01-01')) is null and 
                    src:MDS_DBLASTUPDATEDDATE is not null) THEN
                    'MDS_DBLASTUPDATEDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:MDS_DBLASTUPDATEDDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MDS_GRIDS_PROCESSED), '0'), 38, 10) is null and 
                    src:MDS_GRIDS_PROCESSED is not null) THEN
                    'MDS_GRIDS_PROCESSED contains a non-numeric value : \'' || AS_VARCHAR(src:MDS_GRIDS_PROCESSED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MDS_LASTSAVED), '1900-01-01')) is null and 
                    src:MDS_LASTSAVED is not null) THEN
                    'MDS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:MDS_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MDS_SYNCID), '0'), 38, 10) is null and 
                    src:MDS_SYNCID is not null) THEN
                    'MDS_SYNCID contains a non-numeric value : \'' || AS_VARCHAR(src:MDS_SYNCID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MDS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:MDS_UPDATECOUNT is not null) THEN
                    'MDS_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:MDS_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MDS_LASTSAVED), '1900-01-01')) is null and 
                src:MDS_LASTSAVED is not null) THEN
                'MDS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:MDS_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:MDS_SYNCID  ORDER BY 
            src:MDS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MOBILEDBSYNC as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MDS_DBLASTUPDATEDDATE), '1900-01-01')) is null and 
                    src:MDS_DBLASTUPDATEDDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MDS_GRIDS_PROCESSED), '0'), 38, 10) is null and 
                    src:MDS_GRIDS_PROCESSED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MDS_LASTSAVED), '1900-01-01')) is null and 
                    src:MDS_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MDS_SYNCID), '0'), 38, 10) is null and 
                    src:MDS_SYNCID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MDS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:MDS_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MDS_LASTSAVED), '1900-01-01')) is null and 
                src:MDS_LASTSAVED is not null)
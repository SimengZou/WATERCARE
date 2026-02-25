CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5MOBILESYNCDELETE_ERROR AS SELECT src, 'EAM_R5MOBILESYNCDELETE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MSD_DELETED), '1900-01-01')) is null and 
                    src:MSD_DELETED is not null) THEN
                    'MSD_DELETED contains a non-timestamp value : \'' || AS_VARCHAR(src:MSD_DELETED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MSD_LASTSAVED), '1900-01-01')) is null and 
                    src:MSD_LASTSAVED is not null) THEN
                    'MSD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:MSD_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MSD_PK), '0'), 38, 10) is null and 
                    src:MSD_PK is not null) THEN
                    'MSD_PK contains a non-numeric value : \'' || AS_VARCHAR(src:MSD_PK) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MSD_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:MSD_UPDATECOUNT is not null) THEN
                    'MSD_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:MSD_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MSD_LASTSAVED), '1900-01-01')) is null and 
                src:MSD_LASTSAVED is not null) THEN
                'MSD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:MSD_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:MSD_PK  ORDER BY 
            src:MSD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MOBILESYNCDELETE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MSD_DELETED), '1900-01-01')) is null and 
                    src:MSD_DELETED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MSD_LASTSAVED), '1900-01-01')) is null and 
                    src:MSD_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MSD_PK), '0'), 38, 10) is null and 
                    src:MSD_PK is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MSD_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:MSD_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MSD_LASTSAVED), '1900-01-01')) is null and 
                src:MSD_LASTSAVED is not null)
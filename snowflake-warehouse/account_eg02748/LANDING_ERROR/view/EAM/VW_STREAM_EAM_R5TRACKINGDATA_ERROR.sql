CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5TRACKINGDATA_ERROR AS SELECT src, 'EAM_R5TRACKINGDATA' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TKD_CREATED), '1900-01-01')) is null and 
                    src:TKD_CREATED is not null) THEN
                    'TKD_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:TKD_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TKD_LASTSAVED), '1900-01-01')) is null and 
                    src:TKD_LASTSAVED is not null) THEN
                    'TKD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:TKD_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKD_SESSIONID), '0'), 38, 10) is null and 
                    src:TKD_SESSIONID is not null) THEN
                    'TKD_SESSIONID contains a non-numeric value : \'' || AS_VARCHAR(src:TKD_SESSIONID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TKD_TRACKDATE), '1900-01-01')) is null and 
                    src:TKD_TRACKDATE is not null) THEN
                    'TKD_TRACKDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:TKD_TRACKDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKD_TRANSID), '0'), 38, 10) is null and 
                    src:TKD_TRANSID is not null) THEN
                    'TKD_TRANSID contains a non-numeric value : \'' || AS_VARCHAR(src:TKD_TRANSID) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TKD_LASTSAVED), '1900-01-01')) is null and 
                src:TKD_LASTSAVED is not null) THEN
                'TKD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:TKD_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:TKD_TRANSID  ORDER BY 
            src:TKD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5TRACKINGDATA as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TKD_CREATED), '1900-01-01')) is null and 
                    src:TKD_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TKD_LASTSAVED), '1900-01-01')) is null and 
                    src:TKD_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKD_SESSIONID), '0'), 38, 10) is null and 
                    src:TKD_SESSIONID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TKD_TRACKDATE), '1900-01-01')) is null and 
                    src:TKD_TRACKDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TKD_TRANSID), '0'), 38, 10) is null and 
                    src:TKD_TRANSID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TKD_LASTSAVED), '1900-01-01')) is null and 
                src:TKD_LASTSAVED is not null)
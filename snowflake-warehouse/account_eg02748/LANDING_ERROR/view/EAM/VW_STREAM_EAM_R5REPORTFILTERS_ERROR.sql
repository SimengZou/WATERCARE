CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5REPORTFILTERS_ERROR AS SELECT src, 'EAM_R5REPORTFILTERS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFI_FROMDATE), '1900-01-01')) is null and 
                    src:RFI_FROMDATE is not null) THEN
                    'RFI_FROMDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:RFI_FROMDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFI_LASTSAVED), '1900-01-01')) is null and 
                    src:RFI_LASTSAVED is not null) THEN
                    'RFI_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:RFI_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFI_TODATE), '1900-01-01')) is null and 
                    src:RFI_TODATE is not null) THEN
                    'RFI_TODATE contains a non-timestamp value : \'' || AS_VARCHAR(src:RFI_TODATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RFI_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:RFI_UPDATECOUNT is not null) THEN
                    'RFI_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:RFI_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFI_UPDATED), '1900-01-01')) is null and 
                    src:RFI_UPDATED is not null) THEN
                    'RFI_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:RFI_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFI_LASTSAVED), '1900-01-01')) is null and 
                src:RFI_LASTSAVED is not null) THEN
                'RFI_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:RFI_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:RFI_FUNC,
                src:RFI_NAME,
                src:RFI_USER  ORDER BY 
            src:RFI_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5REPORTFILTERS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFI_FROMDATE), '1900-01-01')) is null and 
                    src:RFI_FROMDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFI_LASTSAVED), '1900-01-01')) is null and 
                    src:RFI_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFI_TODATE), '1900-01-01')) is null and 
                    src:RFI_TODATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RFI_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:RFI_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFI_UPDATED), '1900-01-01')) is null and 
                    src:RFI_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:RFI_LASTSAVED), '1900-01-01')) is null and 
                src:RFI_LASTSAVED is not null)
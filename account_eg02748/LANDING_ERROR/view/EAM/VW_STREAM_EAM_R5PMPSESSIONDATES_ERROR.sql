CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5PMPSESSIONDATES_ERROR AS SELECT src, 'EAM_R5PMPSESSIONDATES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPD_DUEDATE), '1900-01-01')) is null and 
                    src:PPD_DUEDATE is not null) THEN
                    'PPD_DUEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:PPD_DUEDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPD_LASTSAVED), '1900-01-01')) is null and 
                    src:PPD_LASTSAVED is not null) THEN
                    'PPD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PPD_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPD_LINE), '0'), 38, 10) is null and 
                    src:PPD_LINE is not null) THEN
                    'PPD_LINE contains a non-numeric value : \'' || AS_VARCHAR(src:PPD_LINE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPD_PPSPK), '0'), 38, 10) is null and 
                    src:PPD_PPSPK is not null) THEN
                    'PPD_PPSPK contains a non-numeric value : \'' || AS_VARCHAR(src:PPD_PPSPK) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPD_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PPD_UPDATECOUNT is not null) THEN
                    'PPD_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PPD_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPD_LASTSAVED), '1900-01-01')) is null and 
                src:PPD_LASTSAVED is not null) THEN
                'PPD_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PPD_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PPD_DUEDATE,
                src:PPD_LINE,
                src:PPD_PPSPK  ORDER BY 
            src:PPD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PMPSESSIONDATES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPD_DUEDATE), '1900-01-01')) is null and 
                    src:PPD_DUEDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPD_LASTSAVED), '1900-01-01')) is null and 
                    src:PPD_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPD_LINE), '0'), 38, 10) is null and 
                    src:PPD_LINE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPD_PPSPK), '0'), 38, 10) is null and 
                    src:PPD_PPSPK is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPD_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PPD_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPD_LASTSAVED), '1900-01-01')) is null and 
                src:PPD_LASTSAVED is not null)
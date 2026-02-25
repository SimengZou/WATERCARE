CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5SCWORKORDERCOST_ERROR AS SELECT src, 'EAM_R5SCWORKORDERCOST' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CWS_COST), '0'), 38, 10) is null and 
                    src:CWS_COST is not null) THEN
                    'CWS_COST contains a non-numeric value : \'' || AS_VARCHAR(src:CWS_COST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CWS_COSTDEFCURR), '0'), 38, 10) is null and 
                    src:CWS_COSTDEFCURR is not null) THEN
                    'CWS_COSTDEFCURR contains a non-numeric value : \'' || AS_VARCHAR(src:CWS_COSTDEFCURR) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CWS_DATE), '1900-01-01')) is null and 
                    src:CWS_DATE is not null) THEN
                    'CWS_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:CWS_DATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CWS_LASTSAVED), '1900-01-01')) is null and 
                    src:CWS_LASTSAVED is not null) THEN
                    'CWS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CWS_LASTSAVED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CWS_LASTSAVED), '1900-01-01')) is null and 
                src:CWS_LASTSAVED is not null) THEN
                'CWS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:CWS_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:CWS_DATE,
                src:CWS_JOBTYPE,
                src:CWS_ORG  ORDER BY 
            src:CWS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5SCWORKORDERCOST as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CWS_COST), '0'), 38, 10) is null and 
                    src:CWS_COST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CWS_COSTDEFCURR), '0'), 38, 10) is null and 
                    src:CWS_COSTDEFCURR is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CWS_DATE), '1900-01-01')) is null and 
                    src:CWS_DATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CWS_LASTSAVED), '1900-01-01')) is null and 
                    src:CWS_LASTSAVED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CWS_LASTSAVED), '1900-01-01')) is null and 
                src:CWS_LASTSAVED is not null)
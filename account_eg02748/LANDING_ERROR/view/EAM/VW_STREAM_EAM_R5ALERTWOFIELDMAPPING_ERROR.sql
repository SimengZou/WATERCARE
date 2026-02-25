CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5ALERTWOFIELDMAPPING_ERROR AS SELECT src, 'EAM_R5ALERTWOFIELDMAPPING' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AWF_BOILERTEXTNUMBER), '0'), 38, 10) is null and 
                    src:AWF_BOILERTEXTNUMBER is not null) THEN
                    'AWF_BOILERTEXTNUMBER contains a non-numeric value : \'' || AS_VARCHAR(src:AWF_BOILERTEXTNUMBER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AWF_GRIDFIELD), '0'), 38, 10) is null and 
                    src:AWF_GRIDFIELD is not null) THEN
                    'AWF_GRIDFIELD contains a non-numeric value : \'' || AS_VARCHAR(src:AWF_GRIDFIELD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AWF_LASTSAVED), '1900-01-01')) is null and 
                    src:AWF_LASTSAVED is not null) THEN
                    'AWF_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:AWF_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AWF_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:AWF_UPDATECOUNT is not null) THEN
                    'AWF_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:AWF_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AWF_LASTSAVED), '1900-01-01')) is null and 
                src:AWF_LASTSAVED is not null) THEN
                'AWF_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:AWF_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:AWF_ALERT,
                src:AWF_WOFIELD  ORDER BY 
            src:AWF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTWOFIELDMAPPING as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AWF_BOILERTEXTNUMBER), '0'), 38, 10) is null and 
                    src:AWF_BOILERTEXTNUMBER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AWF_GRIDFIELD), '0'), 38, 10) is null and 
                    src:AWF_GRIDFIELD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AWF_LASTSAVED), '1900-01-01')) is null and 
                    src:AWF_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AWF_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:AWF_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AWF_LASTSAVED), '1900-01-01')) is null and 
                src:AWF_LASTSAVED is not null)
CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5GROUPS_ERROR AS SELECT src, 'EAM_R5GROUPS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UGR_CREATED), '1900-01-01')) is null and 
                    src:UGR_CREATED is not null) THEN
                    'UGR_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:UGR_CREATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UGR_EQUIPFORDATASPY), '0'), 38, 10) is null and 
                    src:UGR_EQUIPFORDATASPY is not null) THEN
                    'UGR_EQUIPFORDATASPY contains a non-numeric value : \'' || AS_VARCHAR(src:UGR_EQUIPFORDATASPY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UGR_FORDATASPY), '0'), 38, 10) is null and 
                    src:UGR_FORDATASPY is not null) THEN
                    'UGR_FORDATASPY contains a non-numeric value : \'' || AS_VARCHAR(src:UGR_FORDATASPY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UGR_FORLATESTDAYS), '0'), 38, 10) is null and 
                    src:UGR_FORLATESTDAYS is not null) THEN
                    'UGR_FORLATESTDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:UGR_FORLATESTDAYS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UGR_LASTSAVED), '1900-01-01')) is null and 
                    src:UGR_LASTSAVED is not null) THEN
                    'UGR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:UGR_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UGR_MAXINSTOCKANDQUANTITY), '0'), 38, 10) is null and 
                    src:UGR_MAXINSTOCKANDQUANTITY is not null) THEN
                    'UGR_MAXINSTOCKANDQUANTITY contains a non-numeric value : \'' || AS_VARCHAR(src:UGR_MAXINSTOCKANDQUANTITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UGR_MTRREADINGLATESTDAYS), '0'), 38, 10) is null and 
                    src:UGR_MTRREADINGLATESTDAYS is not null) THEN
                    'UGR_MTRREADINGLATESTDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:UGR_MTRREADINGLATESTDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UGR_SESSIONTIMEOUT), '0'), 38, 10) is null and 
                    src:UGR_SESSIONTIMEOUT is not null) THEN
                    'UGR_SESSIONTIMEOUT contains a non-numeric value : \'' || AS_VARCHAR(src:UGR_SESSIONTIMEOUT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UGR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:UGR_UPDATECOUNT is not null) THEN
                    'UGR_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:UGR_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UGR_UPDATED), '1900-01-01')) is null and 
                    src:UGR_UPDATED is not null) THEN
                    'UGR_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:UGR_UPDATED) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UGR_LASTSAVED), '1900-01-01')) is null and 
                src:UGR_LASTSAVED is not null) THEN
                'UGR_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:UGR_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:UGR_CODE  ORDER BY 
            src:UGR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5GROUPS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UGR_CREATED), '1900-01-01')) is null and 
                    src:UGR_CREATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UGR_EQUIPFORDATASPY), '0'), 38, 10) is null and 
                    src:UGR_EQUIPFORDATASPY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UGR_FORDATASPY), '0'), 38, 10) is null and 
                    src:UGR_FORDATASPY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UGR_FORLATESTDAYS), '0'), 38, 10) is null and 
                    src:UGR_FORLATESTDAYS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UGR_LASTSAVED), '1900-01-01')) is null and 
                    src:UGR_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UGR_MAXINSTOCKANDQUANTITY), '0'), 38, 10) is null and 
                    src:UGR_MAXINSTOCKANDQUANTITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UGR_MTRREADINGLATESTDAYS), '0'), 38, 10) is null and 
                    src:UGR_MTRREADINGLATESTDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UGR_SESSIONTIMEOUT), '0'), 38, 10) is null and 
                    src:UGR_SESSIONTIMEOUT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UGR_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:UGR_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UGR_UPDATED), '1900-01-01')) is null and 
                    src:UGR_UPDATED is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:UGR_LASTSAVED), '1900-01-01')) is null and 
                src:UGR_LASTSAVED is not null)
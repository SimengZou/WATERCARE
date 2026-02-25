CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5PMPLANSESSION_ERROR AS SELECT src, 'EAM_R5PMPLANSESSION' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPS_DUEDATE), '1900-01-01')) is null and 
                    src:PPS_DUEDATE is not null) THEN
                    'PPS_DUEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:PPS_DUEDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPS_DUEDAYOFWEEK), '0'), 38, 10) is null and 
                    src:PPS_DUEDAYOFWEEK is not null) THEN
                    'PPS_DUEDAYOFWEEK contains a non-numeric value : \'' || AS_VARCHAR(src:PPS_DUEDAYOFWEEK) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPS_LASTSAVED), '1900-01-01')) is null and 
                    src:PPS_LASTSAVED is not null) THEN
                    'PPS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PPS_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPS_PK), '0'), 38, 10) is null and 
                    src:PPS_PK is not null) THEN
                    'PPS_PK contains a non-numeric value : \'' || AS_VARCHAR(src:PPS_PK) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPS_PMREVISION), '0'), 38, 10) is null and 
                    src:PPS_PMREVISION is not null) THEN
                    'PPS_PMREVISION contains a non-numeric value : \'' || AS_VARCHAR(src:PPS_PMREVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPS_PPOPK), '0'), 38, 10) is null and 
                    src:PPS_PPOPK is not null) THEN
                    'PPS_PPOPK contains a non-numeric value : \'' || AS_VARCHAR(src:PPS_PPOPK) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPS_SESSIONID), '0'), 38, 10) is null and 
                    src:PPS_SESSIONID is not null) THEN
                    'PPS_SESSIONID contains a non-numeric value : \'' || AS_VARCHAR(src:PPS_SESSIONID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PPS_UPDATECOUNT is not null) THEN
                    'PPS_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PPS_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPS_LASTSAVED), '1900-01-01')) is null and 
                src:PPS_LASTSAVED is not null) THEN
                'PPS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PPS_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PPS_PK  ORDER BY 
            src:PPS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PMPLANSESSION as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPS_DUEDATE), '1900-01-01')) is null and 
                    src:PPS_DUEDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPS_DUEDAYOFWEEK), '0'), 38, 10) is null and 
                    src:PPS_DUEDAYOFWEEK is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPS_LASTSAVED), '1900-01-01')) is null and 
                    src:PPS_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPS_PK), '0'), 38, 10) is null and 
                    src:PPS_PK is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPS_PMREVISION), '0'), 38, 10) is null and 
                    src:PPS_PMREVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPS_PPOPK), '0'), 38, 10) is null and 
                    src:PPS_PPOPK is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPS_SESSIONID), '0'), 38, 10) is null and 
                    src:PPS_SESSIONID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PPS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PPS_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PPS_LASTSAVED), '1900-01-01')) is null and 
                src:PPS_LASTSAVED is not null)
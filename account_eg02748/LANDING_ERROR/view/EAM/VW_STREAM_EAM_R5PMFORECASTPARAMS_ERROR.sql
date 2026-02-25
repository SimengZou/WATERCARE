CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5PMFORECASTPARAMS_ERROR AS SELECT src, 'EAM_R5PMFORECASTPARAMS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PFP_CREATED), '1900-01-01')) is null and 
                    src:PFP_CREATED is not null) THEN
                    'PFP_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:PFP_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PFP_ENDDATE), '1900-01-01')) is null and 
                    src:PFP_ENDDATE is not null) THEN
                    'PFP_ENDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:PFP_ENDDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PFP_LASTSAVED), '1900-01-01')) is null and 
                    src:PFP_LASTSAVED is not null) THEN
                    'PFP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PFP_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PFP_MIN_FREQ), '0'), 38, 10) is null and 
                    src:PFP_MIN_FREQ is not null) THEN
                    'PFP_MIN_FREQ contains a non-numeric value : \'' || AS_VARCHAR(src:PFP_MIN_FREQ) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PFP_PAGESIZE), '0'), 38, 10) is null and 
                    src:PFP_PAGESIZE is not null) THEN
                    'PFP_PAGESIZE contains a non-numeric value : \'' || AS_VARCHAR(src:PFP_PAGESIZE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PFP_SCREENHSPLIT), '0'), 38, 10) is null and 
                    src:PFP_SCREENHSPLIT is not null) THEN
                    'PFP_SCREENHSPLIT contains a non-numeric value : \'' || AS_VARCHAR(src:PFP_SCREENHSPLIT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PFP_SESSIONID), '0'), 38, 10) is null and 
                    src:PFP_SESSIONID is not null) THEN
                    'PFP_SESSIONID contains a non-numeric value : \'' || AS_VARCHAR(src:PFP_SESSIONID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PFP_STARTDATE), '1900-01-01')) is null and 
                    src:PFP_STARTDATE is not null) THEN
                    'PFP_STARTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:PFP_STARTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PFP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PFP_UPDATECOUNT is not null) THEN
                    'PFP_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PFP_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PFP_WORKHOURS), '0'), 38, 10) is null and 
                    src:PFP_WORKHOURS is not null) THEN
                    'PFP_WORKHOURS contains a non-numeric value : \'' || AS_VARCHAR(src:PFP_WORKHOURS) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PFP_LASTSAVED), '1900-01-01')) is null and 
                src:PFP_LASTSAVED is not null) THEN
                'PFP_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:PFP_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:PFP_CODE  ORDER BY 
            src:PFP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PMFORECASTPARAMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PFP_CREATED), '1900-01-01')) is null and 
                    src:PFP_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PFP_ENDDATE), '1900-01-01')) is null and 
                    src:PFP_ENDDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PFP_LASTSAVED), '1900-01-01')) is null and 
                    src:PFP_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PFP_MIN_FREQ), '0'), 38, 10) is null and 
                    src:PFP_MIN_FREQ is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PFP_PAGESIZE), '0'), 38, 10) is null and 
                    src:PFP_PAGESIZE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PFP_SCREENHSPLIT), '0'), 38, 10) is null and 
                    src:PFP_SCREENHSPLIT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PFP_SESSIONID), '0'), 38, 10) is null and 
                    src:PFP_SESSIONID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PFP_STARTDATE), '1900-01-01')) is null and 
                    src:PFP_STARTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PFP_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:PFP_UPDATECOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PFP_WORKHOURS), '0'), 38, 10) is null and 
                    src:PFP_WORKHOURS is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PFP_LASTSAVED), '1900-01-01')) is null and 
                src:PFP_LASTSAVED is not null)
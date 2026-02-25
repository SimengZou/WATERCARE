CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5WOLABORSCHEDPARAMS_ERROR AS SELECT src, 'EAM_R5WOLABORSCHEDPARAMS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_ACTENDDATE), '1900-01-01')) is null and 
                    src:WLS_ACTENDDATE is not null) THEN
                    'WLS_ACTENDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:WLS_ACTENDDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_ACTSTARTDATE), '1900-01-01')) is null and 
                    src:WLS_ACTSTARTDATE is not null) THEN
                    'WLS_ACTSTARTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:WLS_ACTSTARTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WLS_CAMPAIGN_LINE), '0'), 38, 10) is null and 
                    src:WLS_CAMPAIGN_LINE is not null) THEN
                    'WLS_CAMPAIGN_LINE contains a non-numeric value : \'' || AS_VARCHAR(src:WLS_CAMPAIGN_LINE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_CREATED), '1900-01-01')) is null and 
                    src:WLS_CREATED is not null) THEN
                    'WLS_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:WLS_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_DATALASTGEN), '1900-01-01')) is null and 
                    src:WLS_DATALASTGEN is not null) THEN
                    'WLS_DATALASTGEN contains a non-timestamp value : \'' || AS_VARCHAR(src:WLS_DATALASTGEN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_GENAVAILTHROUGH), '1900-01-01')) is null and 
                    src:WLS_GENAVAILTHROUGH is not null) THEN
                    'WLS_GENAVAILTHROUGH contains a non-timestamp value : \'' || AS_VARCHAR(src:WLS_GENAVAILTHROUGH) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_LASTSAVED), '1900-01-01')) is null and 
                    src:WLS_LASTSAVED is not null) THEN
                    'WLS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:WLS_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_LISTLASTREFRESHED), '1900-01-01')) is null and 
                    src:WLS_LISTLASTREFRESHED is not null) THEN
                    'WLS_LISTLASTREFRESHED contains a non-timestamp value : \'' || AS_VARCHAR(src:WLS_LISTLASTREFRESHED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WLS_MAXCALCPRIORITY), '0'), 38, 10) is null and 
                    src:WLS_MAXCALCPRIORITY is not null) THEN
                    'WLS_MAXCALCPRIORITY contains a non-numeric value : \'' || AS_VARCHAR(src:WLS_MAXCALCPRIORITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WLS_MP_SEQ), '0'), 38, 10) is null and 
                    src:WLS_MP_SEQ is not null) THEN
                    'WLS_MP_SEQ contains a non-numeric value : \'' || AS_VARCHAR(src:WLS_MP_SEQ) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_SCHEDULINGSTARTDATE), '1900-01-01')) is null and 
                    src:WLS_SCHEDULINGSTARTDATE is not null) THEN
                    'WLS_SCHEDULINGSTARTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:WLS_SCHEDULINGSTARTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WLS_SESSIONID), '0'), 38, 10) is null and 
                    src:WLS_SESSIONID is not null) THEN
                    'WLS_SESSIONID contains a non-numeric value : \'' || AS_VARCHAR(src:WLS_SESSIONID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WLS_SHIFTDURATION), '0'), 38, 10) is null and 
                    src:WLS_SHIFTDURATION is not null) THEN
                    'WLS_SHIFTDURATION contains a non-numeric value : \'' || AS_VARCHAR(src:WLS_SHIFTDURATION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_SHIFTSTART), '1900-01-01')) is null and 
                    src:WLS_SHIFTSTART is not null) THEN
                    'WLS_SHIFTSTART contains a non-timestamp value : \'' || AS_VARCHAR(src:WLS_SHIFTSTART) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WLS_THRESHOLDPERCENT), '0'), 38, 10) is null and 
                    src:WLS_THRESHOLDPERCENT is not null) THEN
                    'WLS_THRESHOLDPERCENT contains a non-numeric value : \'' || AS_VARCHAR(src:WLS_THRESHOLDPERCENT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WLS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:WLS_UPDATECOUNT is not null) THEN
                    'WLS_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:WLS_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_UPDATED), '1900-01-01')) is null and 
                    src:WLS_UPDATED is not null) THEN
                    'WLS_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:WLS_UPDATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_WOSCHEDENDDATE), '1900-01-01')) is null and 
                    src:WLS_WOSCHEDENDDATE is not null) THEN
                    'WLS_WOSCHEDENDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:WLS_WOSCHEDENDDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_WOSCHEDSTARTDATE), '1900-01-01')) is null and 
                    src:WLS_WOSCHEDSTARTDATE is not null) THEN
                    'WLS_WOSCHEDSTARTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:WLS_WOSCHEDSTARTDATE) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_LASTSAVED), '1900-01-01')) is null and 
                src:WLS_LASTSAVED is not null) THEN
                'WLS_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:WLS_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:WLS_CODE  ORDER BY 
            src:WLS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5WOLABORSCHEDPARAMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_ACTENDDATE), '1900-01-01')) is null and 
                    src:WLS_ACTENDDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_ACTSTARTDATE), '1900-01-01')) is null and 
                    src:WLS_ACTSTARTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WLS_CAMPAIGN_LINE), '0'), 38, 10) is null and 
                    src:WLS_CAMPAIGN_LINE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_CREATED), '1900-01-01')) is null and 
                    src:WLS_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_DATALASTGEN), '1900-01-01')) is null and 
                    src:WLS_DATALASTGEN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_GENAVAILTHROUGH), '1900-01-01')) is null and 
                    src:WLS_GENAVAILTHROUGH is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_LASTSAVED), '1900-01-01')) is null and 
                    src:WLS_LASTSAVED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_LISTLASTREFRESHED), '1900-01-01')) is null and 
                    src:WLS_LISTLASTREFRESHED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WLS_MAXCALCPRIORITY), '0'), 38, 10) is null and 
                    src:WLS_MAXCALCPRIORITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WLS_MP_SEQ), '0'), 38, 10) is null and 
                    src:WLS_MP_SEQ is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_SCHEDULINGSTARTDATE), '1900-01-01')) is null and 
                    src:WLS_SCHEDULINGSTARTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WLS_SESSIONID), '0'), 38, 10) is null and 
                    src:WLS_SESSIONID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WLS_SHIFTDURATION), '0'), 38, 10) is null and 
                    src:WLS_SHIFTDURATION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_SHIFTSTART), '1900-01-01')) is null and 
                    src:WLS_SHIFTSTART is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WLS_THRESHOLDPERCENT), '0'), 38, 10) is null and 
                    src:WLS_THRESHOLDPERCENT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WLS_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:WLS_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_UPDATED), '1900-01-01')) is null and 
                    src:WLS_UPDATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_WOSCHEDENDDATE), '1900-01-01')) is null and 
                    src:WLS_WOSCHEDENDDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_WOSCHEDSTARTDATE), '1900-01-01')) is null and 
                    src:WLS_WOSCHEDSTARTDATE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:WLS_LASTSAVED), '1900-01-01')) is null and 
                src:WLS_LASTSAVED is not null)
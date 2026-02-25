CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5IMPORTEXPORTSTATUS_ERROR AS SELECT src, 'EAM_R5IMPORTEXPORTSTATUS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IES_COMPLETED), '1900-01-01')) is null and 
                    src:IES_COMPLETED is not null) THEN
                    'IES_COMPLETED contains a non-timestamp value : \'' || AS_VARCHAR(src:IES_COMPLETED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IES_DATECREATED), '1900-01-01')) is null and 
                    src:IES_DATECREATED is not null) THEN
                    'IES_DATECREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:IES_DATECREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IES_ESTTIMETOEND), '1900-01-01')) is null and 
                    src:IES_ESTTIMETOEND is not null) THEN
                    'IES_ESTTIMETOEND contains a non-timestamp value : \'' || AS_VARCHAR(src:IES_ESTTIMETOEND) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IES_ESTTIMETOSTART), '1900-01-01')) is null and 
                    src:IES_ESTTIMETOSTART is not null) THEN
                    'IES_ESTTIMETOSTART contains a non-timestamp value : \'' || AS_VARCHAR(src:IES_ESTTIMETOSTART) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IES_LASTSAVED), '1900-01-01')) is null and 
                    src:IES_LASTSAVED is not null) THEN
                    'IES_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:IES_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IES_RECORDCOUNT), '0'), 38, 10) is null and 
                    src:IES_RECORDCOUNT is not null) THEN
                    'IES_RECORDCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:IES_RECORDCOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IES_SESSIONID), '0'), 38, 10) is null and 
                    src:IES_SESSIONID is not null) THEN
                    'IES_SESSIONID contains a non-numeric value : \'' || AS_VARCHAR(src:IES_SESSIONID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IES_STARTED), '1900-01-01')) is null and 
                    src:IES_STARTED is not null) THEN
                    'IES_STARTED contains a non-timestamp value : \'' || AS_VARCHAR(src:IES_STARTED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IES_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:IES_UPDATECOUNT is not null) THEN
                    'IES_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:IES_UPDATECOUNT) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IES_LASTSAVED), '1900-01-01')) is null and 
                src:IES_LASTSAVED is not null) THEN
                'IES_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:IES_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:IES_SESSIONID  ORDER BY 
            src:IES_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5IMPORTEXPORTSTATUS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IES_COMPLETED), '1900-01-01')) is null and 
                    src:IES_COMPLETED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IES_DATECREATED), '1900-01-01')) is null and 
                    src:IES_DATECREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IES_ESTTIMETOEND), '1900-01-01')) is null and 
                    src:IES_ESTTIMETOEND is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IES_ESTTIMETOSTART), '1900-01-01')) is null and 
                    src:IES_ESTTIMETOSTART is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IES_LASTSAVED), '1900-01-01')) is null and 
                    src:IES_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IES_RECORDCOUNT), '0'), 38, 10) is null and 
                    src:IES_RECORDCOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IES_SESSIONID), '0'), 38, 10) is null and 
                    src:IES_SESSIONID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IES_STARTED), '1900-01-01')) is null and 
                    src:IES_STARTED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IES_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:IES_UPDATECOUNT is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:IES_LASTSAVED), '1900-01-01')) is null and 
                src:IES_LASTSAVED is not null)
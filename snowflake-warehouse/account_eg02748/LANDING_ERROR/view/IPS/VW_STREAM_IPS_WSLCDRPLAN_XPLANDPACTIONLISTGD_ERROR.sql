CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLCDRPLAN_XPLANDPACTIONLISTGD_ERROR AS SELECT src, 'IPS_WSLCDRPLAN_XPLANDPACTIONLISTGD' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COMPLETEDDATE), '1900-01-01')) is null and 
                    src:COMPLETEDDATE is not null) THEN
                    'COMPLETEDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:COMPLETEDDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CREATEDATE), '1900-01-01')) is null and 
                    src:CREATEDATE is not null) THEN
                    'CREATEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:CREATEDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DUEDATE), '1900-01-01')) is null and 
                    src:DUEDATE is not null) THEN
                    'DUEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DUEDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVIEWID), '0'), 38, 10) is null and 
                    src:REVIEWID is not null) THEN
                    'REVIEWID contains a non-numeric value : \'' || AS_VARCHAR(src:REVIEWID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XPLANDPACTIONLISTGDKEY), '0'), 38, 10) is null and 
                    src:XPLANDPACTIONLISTGDKEY is not null) THEN
                    'XPLANDPACTIONLISTGDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XPLANDPACTIONLISTGDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XPLANDPACTIONLISTKEY), '0'), 38, 10) is null and 
                    src:XPLANDPACTIONLISTKEY is not null) THEN
                    'XPLANDPACTIONLISTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:XPLANDPACTIONLISTKEY) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null) THEN
                'VARIATION_ID contains a non-timestamp value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:XPLANDPACTIONLISTGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRPLAN_XPLANDPACTIONLISTGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COMPLETEDDATE), '1900-01-01')) is null and 
                    src:COMPLETEDDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CREATEDATE), '1900-01-01')) is null and 
                    src:CREATEDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DUEDATE), '1900-01-01')) is null and 
                    src:DUEDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVIEWID), '0'), 38, 10) is null and 
                    src:REVIEWID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XPLANDPACTIONLISTGDKEY), '0'), 38, 10) is null and 
                    src:XPLANDPACTIONLISTGDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XPLANDPACTIONLISTKEY), '0'), 38, 10) is null and 
                    src:XPLANDPACTIONLISTKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)
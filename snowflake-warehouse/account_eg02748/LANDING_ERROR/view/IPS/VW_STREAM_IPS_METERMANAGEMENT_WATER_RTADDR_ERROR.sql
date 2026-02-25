CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_METERMANAGEMENT_WATER_RTADDR_ERROR AS SELECT src, 'IPS_METERMANAGEMENT_WATER_RTADDR' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INDEXNO), '0'), 38, 10) is null and 
                    src:INDEXNO is not null) THEN
                    'INDEXNO contains a non-numeric value : \'' || AS_VARCHAR(src:INDEXNO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) THEN
                    'POSITION contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REREADSRVREQNO), '0'), 38, 10) is null and 
                    src:REREADSRVREQNO is not null) THEN
                    'REREADSRVREQNO contains a non-numeric value : \'' || AS_VARCHAR(src:REREADSRVREQNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROUTEKEY), '0'), 38, 10) is null and 
                    src:ROUTEKEY is not null) THEN
                    'ROUTEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ROUTEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTADDRKEY), '0'), 38, 10) is null and 
                    src:RTADDRKEY is not null) THEN
                    'RTADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RTADDRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEQNO), '0'), 38, 10) is null and 
                    src:SEQNO is not null) THEN
                    'SEQNO contains a non-numeric value : \'' || AS_VARCHAR(src:SEQNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
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
                                    
                src:RTADDRKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_RTADDR as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INDEXNO), '0'), 38, 10) is null and 
                    src:INDEXNO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REREADSRVREQNO), '0'), 38, 10) is null and 
                    src:REREADSRVREQNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROUTEKEY), '0'), 38, 10) is null and 
                    src:ROUTEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RTADDRKEY), '0'), 38, 10) is null and 
                    src:RTADDRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEQNO), '0'), 38, 10) is null and 
                    src:SEQNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)
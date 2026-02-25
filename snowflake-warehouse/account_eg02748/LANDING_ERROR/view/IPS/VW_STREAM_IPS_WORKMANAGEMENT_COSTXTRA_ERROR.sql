CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WORKMANAGEMENT_COSTXTRA_ERROR AS SELECT src, 'IPS_WORKMANAGEMENT_COSTXTRA' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CHRGDTTM), '1900-01-01')) is null and 
                    src:CHRGDTTM is not null) THEN
                    'CHRGDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:CHRGDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CHRGDTTMTO), '1900-01-01')) is null and 
                    src:CHRGDTTMTO is not null) THEN
                    'CHRGDTTMTO contains a non-timestamp value : \'' || AS_VARCHAR(src:CHRGDTTMTO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COSTKEY), '0'), 38, 10) is null and 
                    src:COSTKEY is not null) THEN
                    'COSTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COSTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DSTBGTKEY), '0'), 38, 10) is null and 
                    src:DSTBGTKEY is not null) THEN
                    'DSTBGTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DSTBGTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HISTKEY), '0'), 38, 10) is null and 
                    src:HISTKEY is not null) THEN
                    'HISTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:HISTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATE), '0'), 38, 10) is null and 
                    src:RATE is not null) THEN
                    'RATE contains a non-numeric value : \'' || AS_VARCHAR(src:RATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRCBGTKEY), '0'), 38, 10) is null and 
                    src:SRCBGTKEY is not null) THEN
                    'SRCBGTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SRCBGTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TASKKEY), '0'), 38, 10) is null and 
                    src:TASKKEY is not null) THEN
                    'TASKKEY contains a non-numeric value : \'' || AS_VARCHAR(src:TASKKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTCOST), '0'), 38, 10) is null and 
                    src:TOTCOST is not null) THEN
                    'TOTCOST contains a non-numeric value : \'' || AS_VARCHAR(src:TOTCOST) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USAGE), '0'), 38, 10) is null and 
                    src:USAGE is not null) THEN
                    'USAGE contains a non-numeric value : \'' || AS_VARCHAR(src:USAGE) || '\'' WHEN 
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
                                    
                src:COSTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WORKMANAGEMENT_COSTXTRA as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CHRGDTTM), '1900-01-01')) is null and 
                    src:CHRGDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CHRGDTTMTO), '1900-01-01')) is null and 
                    src:CHRGDTTMTO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COSTKEY), '0'), 38, 10) is null and 
                    src:COSTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DSTBGTKEY), '0'), 38, 10) is null and 
                    src:DSTBGTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HISTKEY), '0'), 38, 10) is null and 
                    src:HISTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATE), '0'), 38, 10) is null and 
                    src:RATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRCBGTKEY), '0'), 38, 10) is null and 
                    src:SRCBGTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TASKKEY), '0'), 38, 10) is null and 
                    src:TASKKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TOTCOST), '0'), 38, 10) is null and 
                    src:TOTCOST is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USAGE), '0'), 38, 10) is null and 
                    src:USAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)
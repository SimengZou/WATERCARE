CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CASHIERING_REGISTERGROUPSETUP_ERROR AS SELECT src, 'IPS_CASHIERING_REGISTERGROUPSETUP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCESSID), '0'), 38, 10) is null and 
                    src:ACCESSID is not null) THEN
                    'ACCESSID contains a non-numeric value : \'' || AS_VARCHAR(src:ACCESSID) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASHLIMITAMT), '0'), 38, 10) is null and 
                    src:CASHLIMITAMT is not null) THEN
                    'CASHLIMITAMT contains a non-numeric value : \'' || AS_VARCHAR(src:CASHLIMITAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASHSETUP), '0'), 38, 10) is null and 
                    src:CASHSETUP is not null) THEN
                    'CASHSETUP contains a non-numeric value : \'' || AS_VARCHAR(src:CASHSETUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHECKSETUP), '0'), 38, 10) is null and 
                    src:CHECKSETUP is not null) THEN
                    'CHECKSETUP contains a non-numeric value : \'' || AS_VARCHAR(src:CHECKSETUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CREDITSETUP), '0'), 38, 10) is null and 
                    src:CREDITSETUP is not null) THEN
                    'CREDITSETUP contains a non-numeric value : \'' || AS_VARCHAR(src:CREDITSETUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEBITSETUP), '0'), 38, 10) is null and 
                    src:DEBITSETUP is not null) THEN
                    'DEBITSETUP contains a non-numeric value : \'' || AS_VARCHAR(src:DEBITSETUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWSETUP), '0'), 38, 10) is null and 
                    src:ESCROWSETUP is not null) THEN
                    'ESCROWSETUP contains a non-numeric value : \'' || AS_VARCHAR(src:ESCROWSETUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MISCSETUP), '0'), 38, 10) is null and 
                    src:MISCSETUP is not null) THEN
                    'MISCSETUP contains a non-numeric value : \'' || AS_VARCHAR(src:MISCSETUP) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGISTERGROUPSETUPKEY), '0'), 38, 10) is null and 
                    src:REGISTERGROUPSETUPKEY is not null) THEN
                    'REGISTERGROUPSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REGISTERGROUPSETUPKEY) || '\'' WHEN 
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
                                    
                src:REGISTERGROUPSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CASHIERING_REGISTERGROUPSETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCESSID), '0'), 38, 10) is null and 
                    src:ACCESSID is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASHLIMITAMT), '0'), 38, 10) is null and 
                    src:CASHLIMITAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASHSETUP), '0'), 38, 10) is null and 
                    src:CASHSETUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHECKSETUP), '0'), 38, 10) is null and 
                    src:CHECKSETUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CREDITSETUP), '0'), 38, 10) is null and 
                    src:CREDITSETUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEBITSETUP), '0'), 38, 10) is null and 
                    src:DEBITSETUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESCROWSETUP), '0'), 38, 10) is null and 
                    src:ESCROWSETUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MISCSETUP), '0'), 38, 10) is null and 
                    src:MISCSETUP is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGISTERGROUPSETUPKEY), '0'), 38, 10) is null and 
                    src:REGISTERGROUPSETUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)
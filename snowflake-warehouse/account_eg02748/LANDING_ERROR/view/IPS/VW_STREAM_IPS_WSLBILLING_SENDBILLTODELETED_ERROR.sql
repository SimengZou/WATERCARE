CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLBILLING_SENDBILLTODELETED_ERROR AS SELECT src, 'IPS_WSLBILLING_SENDBILLTODELETED' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSCONTACTKEY), '0'), 38, 10) is null and 
                    src:ADDRESSCONTACTKEY is not null) THEN
                    'ADDRESSCONTACTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSCONTACTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASSBARCODE), '0'), 38, 10) is null and 
                    src:CASSBARCODE is not null) THEN
                    'CASSBARCODE contains a non-numeric value : \'' || AS_VARCHAR(src:CASSBARCODE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASSVER), '0'), 38, 10) is null and 
                    src:CASSVER is not null) THEN
                    'CASSVER contains a non-numeric value : \'' || AS_VARCHAR(src:CASSVER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELIVERYOPTION), '0'), 38, 10) is null and 
                    src:DELIVERYOPTION is not null) THEN
                    'DELIVERYOPTION contains a non-numeric value : \'' || AS_VARCHAR(src:DELIVERYOPTION) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDATE), '1900-01-01')) is null and 
                    src:EFFECTIVEDATE is not null) THEN
                    'EFFECTIVEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFECTIVEDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORIGADDDTTM), '1900-01-01')) is null and 
                    src:ORIGADDDTTM is not null) THEN
                    'ORIGADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ORIGADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORIGMODDTTM), '1900-01-01')) is null and 
                    src:ORIGMODDTTM is not null) THEN
                    'ORIGMODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ORIGMODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEASCASSVER), '0'), 38, 10) is null and 
                    src:SEASCASSVER is not null) THEN
                    'SEASCASSVER contains a non-numeric value : \'' || AS_VARCHAR(src:SEASCASSVER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SEASFROMDATE), '1900-01-01')) is null and 
                    src:SEASFROMDATE is not null) THEN
                    'SEASFROMDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:SEASFROMDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SEASTODATE), '1900-01-01')) is null and 
                    src:SEASTODATE is not null) THEN
                    'SEASTODATE contains a non-timestamp value : \'' || AS_VARCHAR(src:SEASTODATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SENDBILLTODELETEDKEY), '0'), 38, 10) is null and 
                    src:SENDBILLTODELETEDKEY is not null) THEN
                    'SENDBILLTODELETEDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SENDBILLTODELETEDKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SENDBILLTOKEY), '0'), 38, 10) is null and 
                    src:SENDBILLTOKEY is not null) THEN
                    'SENDBILLTOKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SENDBILLTOKEY) || '\'' WHEN 
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
                                    
                src:SENDBILLTODELETEDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLBILLING_SENDBILLTODELETED as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSCONTACTKEY), '0'), 38, 10) is null and 
                    src:ADDRESSCONTACTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASSBARCODE), '0'), 38, 10) is null and 
                    src:CASSBARCODE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CASSVER), '0'), 38, 10) is null and 
                    src:CASSVER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELIVERYOPTION), '0'), 38, 10) is null and 
                    src:DELIVERYOPTION is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDATE), '1900-01-01')) is null and 
                    src:EFFECTIVEDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORIGADDDTTM), '1900-01-01')) is null and 
                    src:ORIGADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ORIGMODDTTM), '1900-01-01')) is null and 
                    src:ORIGMODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEASCASSVER), '0'), 38, 10) is null and 
                    src:SEASCASSVER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SEASFROMDATE), '1900-01-01')) is null and 
                    src:SEASFROMDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SEASTODATE), '1900-01-01')) is null and 
                    src:SEASTODATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SENDBILLTODELETEDKEY), '0'), 38, 10) is null and 
                    src:SENDBILLTODELETEDKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SENDBILLTOKEY), '0'), 38, 10) is null and 
                    src:SENDBILLTOKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)
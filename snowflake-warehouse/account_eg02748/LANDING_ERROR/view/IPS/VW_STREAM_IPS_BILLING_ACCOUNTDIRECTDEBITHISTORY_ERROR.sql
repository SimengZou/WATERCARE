CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ACCOUNTDIRECTDEBITHISTORY_ERROR AS SELECT src, 'IPS_BILLING_ACCOUNTDIRECTDEBITHISTORY' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTDIRECTDEBITHISTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTDIRECTDEBITHISTKEY is not null) THEN
                    'ACCOUNTDIRECTDEBITHISTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTDIRECTDEBITHISTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTDIRECTDEBITKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTDIRECTDEBITKEY is not null) THEN
                    'ACCOUNTDIRECTDEBITKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTDIRECTDEBITKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) THEN
                    'COMMENTSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMMENTSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONFIGUREDVALUE), '0'), 38, 10) is null and 
                    src:CONFIGUREDVALUE is not null) THEN
                    'CONFIGUREDVALUE contains a non-numeric value : \'' || AS_VARCHAR(src:CONFIGUREDVALUE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEBITAMOUNT), '0'), 38, 10) is null and 
                    src:DEBITAMOUNT is not null) THEN
                    'DEBITAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DEBITAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEBITFREQUENCY), '0'), 38, 10) is null and 
                    src:DEBITFREQUENCY is not null) THEN
                    'DEBITFREQUENCY contains a non-numeric value : \'' || AS_VARCHAR(src:DEBITFREQUENCY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECDEBITCYCLE), '0'), 38, 10) is null and 
                    src:DIRECDEBITCYCLE is not null) THEN
                    'DIRECDEBITCYCLE contains a non-numeric value : \'' || AS_VARCHAR(src:DIRECDEBITCYCLE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITSCHEDULEPLAN), '0'), 38, 10) is null and 
                    src:DIRECTDEBITSCHEDULEPLAN is not null) THEN
                    'DIRECTDEBITSCHEDULEPLAN contains a non-numeric value : \'' || AS_VARCHAR(src:DIRECTDEBITSCHEDULEPLAN) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DONATIONAMOUNT), '0'), 38, 10) is null and 
                    src:DONATIONAMOUNT is not null) THEN
                    'DONATIONAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:DONATIONAMOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDATE), '1900-01-01')) is null and 
                    src:EFFECTIVEDATE is not null) THEN
                    'EFFECTIVEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFECTIVEDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPIREDATE), '1900-01-01')) is null and 
                    src:EXPIREDATE is not null) THEN
                    'EXPIREDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPIREDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXTRACTDATE), '1900-01-01')) is null and 
                    src:EXTRACTDATE is not null) THEN
                    'EXTRACTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXTRACTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IDENTITYBANKINGKEY), '0'), 38, 10) is null and 
                    src:IDENTITYBANKINGKEY is not null) THEN
                    'IDENTITYBANKINGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:IDENTITYBANKINGKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTEXTRACTDATE), '1900-01-01')) is null and 
                    src:LASTEXTRACTDATE is not null) THEN
                    'LASTEXTRACTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:LASTEXTRACTDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEXTEXTRACTDATE), '1900-01-01')) is null and 
                    src:NEXTEXTRACTDATE is not null) THEN
                    'NEXTEXTRACTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:NEXTEXTRACTDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRANSMITDATE), '1900-01-01')) is null and 
                    src:TRANSMITDATE is not null) THEN
                    'TRANSMITDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:TRANSMITDATE) || '\'' WHEN 
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
                                    
                src:ACCOUNTDIRECTDEBITHISTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ACCOUNTDIRECTDEBITHISTORY as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTDIRECTDEBITHISTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTDIRECTDEBITHISTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTDIRECTDEBITKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTDIRECTDEBITKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONFIGUREDVALUE), '0'), 38, 10) is null and 
                    src:CONFIGUREDVALUE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEBITAMOUNT), '0'), 38, 10) is null and 
                    src:DEBITAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEBITFREQUENCY), '0'), 38, 10) is null and 
                    src:DEBITFREQUENCY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECDEBITCYCLE), '0'), 38, 10) is null and 
                    src:DIRECDEBITCYCLE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIRECTDEBITSCHEDULEPLAN), '0'), 38, 10) is null and 
                    src:DIRECTDEBITSCHEDULEPLAN is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DONATIONAMOUNT), '0'), 38, 10) is null and 
                    src:DONATIONAMOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDATE), '1900-01-01')) is null and 
                    src:EFFECTIVEDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPIREDATE), '1900-01-01')) is null and 
                    src:EXPIREDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXTRACTDATE), '1900-01-01')) is null and 
                    src:EXTRACTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IDENTITYBANKINGKEY), '0'), 38, 10) is null and 
                    src:IDENTITYBANKINGKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTEXTRACTDATE), '1900-01-01')) is null and 
                    src:LASTEXTRACTDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NEXTEXTRACTDATE), '1900-01-01')) is null and 
                    src:NEXTEXTRACTDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:TRANSMITDATE), '1900-01-01')) is null and 
                    src:TRANSMITDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)
CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CASHIERING_PAYMENTDISTRIBUTION_ERROR AS SELECT src, 'IPS_CASHIERING_PAYMENTDISTRIBUTION' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHGKEY), '0'), 38, 10) is null and 
                    src:CHGKEY is not null) THEN
                    'CHGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CHGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTAMT), '0'), 38, 10) is null and 
                    src:DISTAMT is not null) THEN
                    'DISTAMT contains a non-numeric value : \'' || AS_VARCHAR(src:DISTAMT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DRWRTRANNO), '0'), 38, 10) is null and 
                    src:DRWRTRANNO is not null) THEN
                    'DRWRTRANNO contains a non-numeric value : \'' || AS_VARCHAR(src:DRWRTRANNO) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYDISTKEY), '0'), 38, 10) is null and 
                    src:PAYDISTKEY is not null) THEN
                    'PAYDISTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYDISTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYSERVERTRANKEY), '0'), 38, 10) is null and 
                    src:PAYSERVERTRANKEY is not null) THEN
                    'PAYSERVERTRANKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PAYSERVERTRANKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFPD), '0'), 38, 10) is null and 
                    src:REFPD is not null) THEN
                    'REFPD contains a non-numeric value : \'' || AS_VARCHAR(src:REFPD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGTRANNO), '0'), 38, 10) is null and 
                    src:REGTRANNO is not null) THEN
                    'REGTRANNO contains a non-numeric value : \'' || AS_VARCHAR(src:REGTRANNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRNNO), '0'), 38, 10) is null and 
                    src:TRNNO is not null) THEN
                    'TRNNO contains a non-numeric value : \'' || AS_VARCHAR(src:TRNNO) || '\'' WHEN 
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
                                    
                src:PAYDISTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CASHIERING_PAYMENTDISTRIBUTION as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHGKEY), '0'), 38, 10) is null and 
                    src:CHGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISTAMT), '0'), 38, 10) is null and 
                    src:DISTAMT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DRWRTRANNO), '0'), 38, 10) is null and 
                    src:DRWRTRANNO is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYDISTKEY), '0'), 38, 10) is null and 
                    src:PAYDISTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYSERVERTRANKEY), '0'), 38, 10) is null and 
                    src:PAYSERVERTRANKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFPD), '0'), 38, 10) is null and 
                    src:REFPD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REGTRANNO), '0'), 38, 10) is null and 
                    src:REGTRANNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRNNO), '0'), 38, 10) is null and 
                    src:TRNNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)
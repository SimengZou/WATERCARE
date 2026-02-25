CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CASHIERING_EXTERNALCHARGE_ERROR AS SELECT src, 'IPS_CASHIERING_EXTERNALCHARGE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHGSUBTOT), '0'), 38, 10) is null and 
                    src:CHGSUBTOT is not null) THEN
                    'CHGSUBTOT contains a non-numeric value : \'' || AS_VARCHAR(src:CHGSUBTOT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHGTAX), '0'), 38, 10) is null and 
                    src:CHGTAX is not null) THEN
                    'CHGTAX contains a non-numeric value : \'' || AS_VARCHAR(src:CHGTAX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHGTAXRATE), '0'), 38, 10) is null and 
                    src:CHGTAXRATE is not null) THEN
                    'CHGTAXRATE contains a non-numeric value : \'' || AS_VARCHAR(src:CHGTAXRATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHGTOT), '0'), 38, 10) is null and 
                    src:CHGTOT is not null) THEN
                    'CHGTOT contains a non-numeric value : \'' || AS_VARCHAR(src:CHGTOT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHGUNITPRC), '0'), 38, 10) is null and 
                    src:CHGUNITPRC is not null) THEN
                    'CHGUNITPRC contains a non-numeric value : \'' || AS_VARCHAR(src:CHGUNITPRC) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXTCHGKEY), '0'), 38, 10) is null and 
                    src:EXTCHGKEY is not null) THEN
                    'EXTCHGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:EXTCHGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXTERNALKEY), '0'), 38, 10) is null and 
                    src:EXTERNALKEY is not null) THEN
                    'EXTERNALKEY contains a non-numeric value : \'' || AS_VARCHAR(src:EXTERNALKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTAMOUNT), '0'), 38, 10) is null and 
                    src:PAYMENTAMOUNT is not null) THEN
                    'PAYMENTAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TENDERTYPE), '0'), 38, 10) is null and 
                    src:TENDERTYPE is not null) THEN
                    'TENDERTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:TENDERTYPE) || '\'' WHEN 
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
                                    
                src:EXTCHGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CASHIERING_EXTERNALCHARGE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHGSUBTOT), '0'), 38, 10) is null and 
                    src:CHGSUBTOT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHGTAX), '0'), 38, 10) is null and 
                    src:CHGTAX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHGTAXRATE), '0'), 38, 10) is null and 
                    src:CHGTAXRATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHGTOT), '0'), 38, 10) is null and 
                    src:CHGTOT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CHGUNITPRC), '0'), 38, 10) is null and 
                    src:CHGUNITPRC is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXTCHGKEY), '0'), 38, 10) is null and 
                    src:EXTCHGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXTERNALKEY), '0'), 38, 10) is null and 
                    src:EXTERNALKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTAMOUNT), '0'), 38, 10) is null and 
                    src:PAYMENTAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TENDERTYPE), '0'), 38, 10) is null and 
                    src:TENDERTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)
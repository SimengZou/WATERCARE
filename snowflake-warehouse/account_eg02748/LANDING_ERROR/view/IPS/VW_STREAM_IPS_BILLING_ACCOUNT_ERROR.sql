CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_ACCOUNT_ERROR AS SELECT src, 'IPS_BILLING_ACCOUNT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) THEN
                    'ACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTTYPE), '0'), 38, 10) is null and 
                    src:ACCOUNTTYPE is not null) THEN
                    'ACCOUNTTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTTYPE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) THEN
                    'ADDRKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AGGREGATEKEY), '0'), 38, 10) is null and 
                    src:AGGREGATEKEY is not null) THEN
                    'AGGREGATEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:AGGREGATEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ARCHIVETHROUGHDATE), '1900-01-01')) is null and 
                    src:ARCHIVETHROUGHDATE is not null) THEN
                    'ARCHIVETHROUGHDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ARCHIVETHROUGHDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BLNGSTDTTM), '1900-01-01')) is null and 
                    src:BLNGSTDTTM is not null) THEN
                    'BLNGSTDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:BLNGSTDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CLOSEDATE), '1900-01-01')) is null and 
                    src:CLOSEDATE is not null) THEN
                    'CLOSEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:CLOSEDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) THEN
                    'COMMENTSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMMENTSKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DEPPDDATE), '1900-01-01')) is null and 
                    src:DEPPDDATE is not null) THEN
                    'DEPPDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:DEPPDDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IDKEY), '0'), 38, 10) is null and 
                    src:IDKEY is not null) THEN
                    'IDKEY contains a non-numeric value : \'' || AS_VARCHAR(src:IDKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIATEDDTTM), '1900-01-01')) is null and 
                    src:INITIATEDDTTM is not null) THEN
                    'INITIATEDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:INITIATEDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEDOUTACCOUNTKEY), '0'), 38, 10) is null and 
                    src:MOVEDOUTACCOUNTKEY is not null) THEN
                    'MOVEDOUTACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEDOUTACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MOVEINADMINHOLDDTTM), '1900-01-01')) is null and 
                    src:MOVEINADMINHOLDDTTM is not null) THEN
                    'MOVEINADMINHOLDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MOVEINADMINHOLDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MOVEINDATE), '1900-01-01')) is null and 
                    src:MOVEINDATE is not null) THEN
                    'MOVEINDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:MOVEINDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINEXCEPTION), '0'), 38, 10) is null and 
                    src:MOVEINEXCEPTION is not null) THEN
                    'MOVEINEXCEPTION contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEINEXCEPTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINSTATUS), '0'), 38, 10) is null and 
                    src:MOVEINSTATUS is not null) THEN
                    'MOVEINSTATUS contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEINSTATUS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MOVEOUTADMINHOLDDTTM), '1900-01-01')) is null and 
                    src:MOVEOUTADMINHOLDDTTM is not null) THEN
                    'MOVEOUTADMINHOLDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MOVEOUTADMINHOLDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MOVEOUTDATE), '1900-01-01')) is null and 
                    src:MOVEOUTDATE is not null) THEN
                    'MOVEOUTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:MOVEOUTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTEXCEPTION), '0'), 38, 10) is null and 
                    src:MOVEOUTEXCEPTION is not null) THEN
                    'MOVEOUTEXCEPTION contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEOUTEXCEPTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTSTATUS), '0'), 38, 10) is null and 
                    src:MOVEOUTSTATUS is not null) THEN
                    'MOVEOUTSTATUS contains a non-numeric value : \'' || AS_VARCHAR(src:MOVEOUTSTATUS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NOTICEDTTM), '1900-01-01')) is null and 
                    src:NOTICEDTTM is not null) THEN
                    'NOTICEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:NOTICEDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL1), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL1 is not null) THEN
                    'NUMERICCONSVAL1 contains a non-numeric value : \'' || AS_VARCHAR(src:NUMERICCONSVAL1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL2), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL2 is not null) THEN
                    'NUMERICCONSVAL2 contains a non-numeric value : \'' || AS_VARCHAR(src:NUMERICCONSVAL2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL3), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL3 is not null) THEN
                    'NUMERICCONSVAL3 contains a non-numeric value : \'' || AS_VARCHAR(src:NUMERICCONSVAL3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL4), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL4 is not null) THEN
                    'NUMERICCONSVAL4 contains a non-numeric value : \'' || AS_VARCHAR(src:NUMERICCONSVAL4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL5), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL5 is not null) THEN
                    'NUMERICCONSVAL5 contains a non-numeric value : \'' || AS_VARCHAR(src:NUMERICCONSVAL5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL6), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL6 is not null) THEN
                    'NUMERICCONSVAL6 contains a non-numeric value : \'' || AS_VARCHAR(src:NUMERICCONSVAL6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL7), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL7 is not null) THEN
                    'NUMERICCONSVAL7 contains a non-numeric value : \'' || AS_VARCHAR(src:NUMERICCONSVAL7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL8), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL8 is not null) THEN
                    'NUMERICCONSVAL8 contains a non-numeric value : \'' || AS_VARCHAR(src:NUMERICCONSVAL8) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OPENDATE), '1900-01-01')) is null and 
                    src:OPENDATE is not null) THEN
                    'OPENDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:OPENDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARCELKEY), '0'), 38, 10) is null and 
                    src:PARCELKEY is not null) THEN
                    'PARCELKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PARCELKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYKEY), '0'), 38, 10) is null and 
                    src:PROPERTYKEY is not null) THEN
                    'PROPERTYKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PROPERTYKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STATDTTM), '1900-01-01')) is null and 
                    src:STATDTTM is not null) THEN
                    'STATDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:STATDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUSPENDDTTM), '1900-01-01')) is null and 
                    src:SUSPENDDTTM is not null) THEN
                    'SUSPENDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:SUSPENDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANSFERREDTO), '0'), 38, 10) is null and 
                    src:TRANSFERREDTO is not null) THEN
                    'TRANSFERREDTO contains a non-numeric value : \'' || AS_VARCHAR(src:TRANSFERREDTO) || '\'' WHEN 
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
                                    
                src:ACCOUNTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ACCOUNT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTTYPE), '0'), 38, 10) is null and 
                    src:ACCOUNTTYPE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is null and 
                    src:ADDRKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AGGREGATEKEY), '0'), 38, 10) is null and 
                    src:AGGREGATEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ARCHIVETHROUGHDATE), '1900-01-01')) is null and 
                    src:ARCHIVETHROUGHDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:BLNGSTDTTM), '1900-01-01')) is null and 
                    src:BLNGSTDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CLOSEDATE), '1900-01-01')) is null and 
                    src:CLOSEDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:DEPPDDATE), '1900-01-01')) is null and 
                    src:DEPPDDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IDKEY), '0'), 38, 10) is null and 
                    src:IDKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INITIATEDDTTM), '1900-01-01')) is null and 
                    src:INITIATEDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEDOUTACCOUNTKEY), '0'), 38, 10) is null and 
                    src:MOVEDOUTACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MOVEINADMINHOLDDTTM), '1900-01-01')) is null and 
                    src:MOVEINADMINHOLDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MOVEINDATE), '1900-01-01')) is null and 
                    src:MOVEINDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINEXCEPTION), '0'), 38, 10) is null and 
                    src:MOVEINEXCEPTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEINSTATUS), '0'), 38, 10) is null and 
                    src:MOVEINSTATUS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MOVEOUTADMINHOLDDTTM), '1900-01-01')) is null and 
                    src:MOVEOUTADMINHOLDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MOVEOUTDATE), '1900-01-01')) is null and 
                    src:MOVEOUTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTEXCEPTION), '0'), 38, 10) is null and 
                    src:MOVEOUTEXCEPTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MOVEOUTSTATUS), '0'), 38, 10) is null and 
                    src:MOVEOUTSTATUS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:NOTICEDTTM), '1900-01-01')) is null and 
                    src:NOTICEDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL1), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL2), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL3), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL4), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL5), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL6), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL7), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMERICCONSVAL8), '0'), 38, 10) is null and 
                    src:NUMERICCONSVAL8 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:OPENDATE), '1900-01-01')) is null and 
                    src:OPENDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARCELKEY), '0'), 38, 10) is null and 
                    src:PARCELKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROPERTYKEY), '0'), 38, 10) is null and 
                    src:PROPERTYKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STATDTTM), '1900-01-01')) is null and 
                    src:STATDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SUSPENDDTTM), '1900-01-01')) is null and 
                    src:SUSPENDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TRANSFERREDTO), '0'), 38, 10) is null and 
                    src:TRANSFERREDTO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)
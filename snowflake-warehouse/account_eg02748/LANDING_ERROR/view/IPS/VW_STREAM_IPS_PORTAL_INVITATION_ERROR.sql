CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_PORTAL_INVITATION_ERROR AS SELECT src, 'IPS_PORTAL_INVITATION' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCEPTEDBY), '0'), 38, 10) is null and 
                    src:ACCEPTEDBY is not null) THEN
                    'ACCEPTEDBY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCEPTEDBY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACCEPTEDDATETIME), '1900-01-01')) is null and 
                    src:ACCEPTEDDATETIME is not null) THEN
                    'ACCEPTEDDATETIME contains a non-timestamp value : \'' || AS_VARCHAR(src:ACCEPTEDDATETIME) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLICANTTYPE), '0'), 38, 10) is null and 
                    src:APPLICANTTYPE is not null) THEN
                    'APPLICANTTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:APPLICANTTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CANCELLEDBY), '0'), 38, 10) is null and 
                    src:CANCELLEDBY is not null) THEN
                    'CANCELLEDBY contains a non-numeric value : \'' || AS_VARCHAR(src:CANCELLEDBY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CANCELLEDDATETIME), '1900-01-01')) is null and 
                    src:CANCELLEDDATETIME is not null) THEN
                    'CANCELLEDDATETIME contains a non-timestamp value : \'' || AS_VARCHAR(src:CANCELLEDDATETIME) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPIREDDATE), '1900-01-01')) is null and 
                    src:EXPIREDDATE is not null) THEN
                    'EXPIREDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPIREDDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INVITATIONKEY), '0'), 38, 10) is null and 
                    src:INVITATIONKEY is not null) THEN
                    'INVITATIONKEY contains a non-numeric value : \'' || AS_VARCHAR(src:INVITATIONKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFERENCEINVITE), '0'), 38, 10) is null and 
                    src:REFERENCEINVITE is not null) THEN
                    'REFERENCEINVITE contains a non-numeric value : \'' || AS_VARCHAR(src:REFERENCEINVITE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SENTBY), '0'), 38, 10) is null and 
                    src:SENTBY is not null) THEN
                    'SENTBY contains a non-numeric value : \'' || AS_VARCHAR(src:SENTBY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SENTDATETIME), '1900-01-01')) is null and 
                    src:SENTDATETIME is not null) THEN
                    'SENTDATETIME contains a non-timestamp value : \'' || AS_VARCHAR(src:SENTDATETIME) || '\'' WHEN 
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
                                    
                src:INVITATIONKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_PORTAL_INVITATION as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCEPTEDBY), '0'), 38, 10) is null and 
                    src:ACCEPTEDBY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACCEPTEDDATETIME), '1900-01-01')) is null and 
                    src:ACCEPTEDDATETIME is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLICANTTYPE), '0'), 38, 10) is null and 
                    src:APPLICANTTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CANCELLEDBY), '0'), 38, 10) is null and 
                    src:CANCELLEDBY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CANCELLEDDATETIME), '1900-01-01')) is null and 
                    src:CANCELLEDDATETIME is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPIREDDATE), '1900-01-01')) is null and 
                    src:EXPIREDDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INVITATIONKEY), '0'), 38, 10) is null and 
                    src:INVITATIONKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REFERENCEINVITE), '0'), 38, 10) is null and 
                    src:REFERENCEINVITE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SENTBY), '0'), 38, 10) is null and 
                    src:SENTBY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:SENTDATETIME), '1900-01-01')) is null and 
                    src:SENTDATETIME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)
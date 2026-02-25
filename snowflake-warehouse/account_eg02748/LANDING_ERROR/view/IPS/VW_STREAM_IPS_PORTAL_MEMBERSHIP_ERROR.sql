CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_PORTAL_MEMBERSHIP_ERROR AS SELECT src, 'IPS_PORTAL_MEMBERSHIP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACTIVATEDDATE), '1900-01-01')) is null and 
                    src:ACTIVATEDDATE is not null) THEN
                    'ACTIVATEDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ACTIVATEDDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APPROVEDDATE), '1900-01-01')) is null and 
                    src:APPROVEDDATE is not null) THEN
                    'APPROVEDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:APPROVEDDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTACT), '0'), 38, 10) is null and 
                    src:CONTACT is not null) THEN
                    'CONTACT contains a non-numeric value : \'' || AS_VARCHAR(src:CONTACT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FAILEDPASSWORDANSWERCOUNT), '0'), 38, 10) is null and 
                    src:FAILEDPASSWORDANSWERCOUNT is not null) THEN
                    'FAILEDPASSWORDANSWERCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:FAILEDPASSWORDANSWERCOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FAILEDPASSWORDCOUNT), '0'), 38, 10) is null and 
                    src:FAILEDPASSWORDCOUNT is not null) THEN
                    'FAILEDPASSWORDCOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:FAILEDPASSWORDCOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTLOGINDATE), '1900-01-01')) is null and 
                    src:LASTLOGINDATE is not null) THEN
                    'LASTLOGINDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:LASTLOGINDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTPASSWORDCHANGEDDATE), '1900-01-01')) is null and 
                    src:LASTPASSWORDCHANGEDDATE is not null) THEN
                    'LASTPASSWORDCHANGEDDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:LASTPASSWORDCHANGEDDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTPASSWORDRESETDATE), '1900-01-01')) is null and 
                    src:LASTPASSWORDRESETDATE is not null) THEN
                    'LASTPASSWORDRESETDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:LASTPASSWORDRESETDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LOCKEDOUTDATE), '1900-01-01')) is null and 
                    src:LOCKEDOUTDATE is not null) THEN
                    'LOCKEDOUTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:LOCKEDOUTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MEMBERSHIPKEY), '0'), 38, 10) is null and 
                    src:MEMBERSHIPKEY is not null) THEN
                    'MEMBERSHIPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MEMBERSHIPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTUSER), '0'), 38, 10) is null and 
                    src:PARENTUSER is not null) THEN
                    'PARENTUSER contains a non-numeric value : \'' || AS_VARCHAR(src:PARENTUSER) || '\'' WHEN 
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
                                    
                src:MEMBERSHIPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_PORTAL_MEMBERSHIP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ACTIVATEDDATE), '1900-01-01')) is null and 
                    src:ACTIVATEDDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:APPROVEDDATE), '1900-01-01')) is null and 
                    src:APPROVEDDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONTACT), '0'), 38, 10) is null and 
                    src:CONTACT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FAILEDPASSWORDANSWERCOUNT), '0'), 38, 10) is null and 
                    src:FAILEDPASSWORDANSWERCOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FAILEDPASSWORDCOUNT), '0'), 38, 10) is null and 
                    src:FAILEDPASSWORDCOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTLOGINDATE), '1900-01-01')) is null and 
                    src:LASTLOGINDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTPASSWORDCHANGEDDATE), '1900-01-01')) is null and 
                    src:LASTPASSWORDCHANGEDDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LASTPASSWORDRESETDATE), '1900-01-01')) is null and 
                    src:LASTPASSWORDRESETDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:LOCKEDOUTDATE), '1900-01-01')) is null and 
                    src:LOCKEDOUTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MEMBERSHIPKEY), '0'), 38, 10) is null and 
                    src:MEMBERSHIPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTUSER), '0'), 38, 10) is null and 
                    src:PARENTUSER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)
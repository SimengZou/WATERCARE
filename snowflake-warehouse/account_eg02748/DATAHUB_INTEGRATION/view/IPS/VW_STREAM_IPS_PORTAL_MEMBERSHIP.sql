CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_PORTAL_MEMBERSHIP AS SELECT
                        src:ACTIVATEDDATE::datetime AS ACTIVATEDDATE, 
                        src:ACTIVATIONCODE::varchar AS ACTIVATIONCODE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROVEDDATE::datetime AS APPROVEDDATE, 
                        src:CONTACT::integer AS CONTACT, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:FAILEDPASSWORDANSWERCOUNT::integer AS FAILEDPASSWORDANSWERCOUNT, 
                        src:FAILEDPASSWORDCOUNT::integer AS FAILEDPASSWORDCOUNT, 
                        src:ISACTIVATED::varchar AS ISACTIVATED, 
                        src:ISAPPROVED::varchar AS ISAPPROVED, 
                        src:ISLOCKEDOUT::varchar AS ISLOCKEDOUT, 
                        src:ISPWDRESETREQ::varchar AS ISPWDRESETREQ, 
                        src:LASTLOGINDATE::datetime AS LASTLOGINDATE, 
                        src:LASTPASSWORDCHANGEDDATE::datetime AS LASTPASSWORDCHANGEDDATE, 
                        src:LASTPASSWORDRESETDATE::datetime AS LASTPASSWORDRESETDATE, 
                        src:LOCKEDOUTDATE::datetime AS LOCKEDOUTDATE, 
                        src:MEMBERSHIPKEY::integer AS MEMBERSHIPKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PARENTUSER::integer AS PARENTUSER, 
                        src:PASSWORDANSWERHASH::varchar AS PASSWORDANSWERHASH, 
                        src:PASSWORDANSWERSALT::varchar AS PASSWORDANSWERSALT, 
                        src:PASSWORDHASH::varchar AS PASSWORDHASH, 
                        src:PASSWORDQUESTION::varchar AS PASSWORDQUESTION, 
                        src:PASSWORDSALT::varchar AS PASSWORDSALT, 
                        src:USERNAME::varchar AS USERNAME, 
                        src:VARIATION_ID::integer AS VARIATION_ID, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
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
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACTIVATEDDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:APPROVEDDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONTACT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FAILEDPASSWORDANSWERCOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FAILEDPASSWORDCOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTLOGINDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTPASSWORDCHANGEDDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTPASSWORDRESETDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LOCKEDOUTDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MEMBERSHIPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PARENTUSER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null
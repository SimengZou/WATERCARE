CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_PORTAL_INVITATION AS SELECT
                        src:ACCEPTEDBY::integer AS ACCEPTEDBY, 
                        src:ACCEPTEDDATETIME::datetime AS ACCEPTEDDATETIME, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPLICANTTYPE::integer AS APPLICANTTYPE, 
                        src:CANCELLEDBY::integer AS CANCELLEDBY, 
                        src:CANCELLEDDATETIME::datetime AS CANCELLEDDATETIME, 
                        src:CAPACITY::varchar AS CAPACITY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:EXPIREDDATE::datetime AS EXPIREDDATE, 
                        src:INSTRUMENTNUMBER::varchar AS INSTRUMENTNUMBER, 
                        src:INVITATIONCODE::varchar AS INVITATIONCODE, 
                        src:INVITATIONKEY::integer AS INVITATIONKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PRODUCTFAMILY::varchar AS PRODUCTFAMILY, 
                        src:REFERENCEINVITE::integer AS REFERENCEINVITE, 
                        src:SENDTO::varchar AS SENDTO, 
                        src:SENTBY::integer AS SENTBY, 
                        src:SENTDATETIME::datetime AS SENTDATETIME, 
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
                                        
                src:INVITATIONKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_PORTAL_INVITATION as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCEPTEDBY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACCEPTEDDATETIME), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APPLICANTTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CANCELLEDBY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CANCELLEDDATETIME), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPIREDDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INVITATIONKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REFERENCEINVITE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SENTBY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SENTDATETIME), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null
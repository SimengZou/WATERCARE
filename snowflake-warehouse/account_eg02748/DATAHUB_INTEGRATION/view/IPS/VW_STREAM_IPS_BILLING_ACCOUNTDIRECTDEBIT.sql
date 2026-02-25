CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_ACCOUNTDIRECTDEBIT AS SELECT
                        src:ACCOUNTDIRECTDEBITKEY::integer AS ACCOUNTDIRECTDEBITKEY, 
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:COMMENTSKEY::integer AS COMMENTSKEY, 
                        src:CONFIGUREDVALUE::integer AS CONFIGUREDVALUE, 
                        src:DEBITAMOUNT::numeric(38, 10) AS DEBITAMOUNT, 
                        src:DEBITFREQUENCY::integer AS DEBITFREQUENCY, 
                        src:DEBITREMANINGAMOUNT::varchar AS DEBITREMANINGAMOUNT, 
                        src:DELETED::boolean AS DELETED, 
                        src:DIRECDEBITCYCLE::integer AS DIRECDEBITCYCLE, 
                        src:DIRECTDEBITSCHEDULEPLAN::integer AS DIRECTDEBITSCHEDULEPLAN, 
                        src:DONATEFLAG::varchar AS DONATEFLAG, 
                        src:DONATIONAMOUNT::numeric(38, 10) AS DONATIONAMOUNT, 
                        src:EFFECTIVEDATE::datetime AS EFFECTIVEDATE, 
                        src:EXPIREDATE::datetime AS EXPIREDATE, 
                        src:EXPIREREASON::varchar AS EXPIREREASON, 
                        src:EXTRACTDATE::datetime AS EXTRACTDATE, 
                        src:IDENTITYBANKINGKEY::integer AS IDENTITYBANKINGKEY, 
                        src:LASTEXTRACTDATE::datetime AS LASTEXTRACTDATE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEXTEXTRACTDATE::datetime AS NEXTEXTRACTDATE, 
                        src:REFRESHBILLAMOUNT::varchar AS REFRESHBILLAMOUNT, 
                        src:TRANSMITDATE::datetime AS TRANSMITDATE, 
                        src:TRANSMITTINGHOLDFLAG::varchar AS TRANSMITTINGHOLDFLAG, 
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
                                        
                src:ACCOUNTDIRECTDEBITKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ACCOUNTDIRECTDEBIT as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTDIRECTDEBITKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONFIGUREDVALUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEBITAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEBITFREQUENCY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIRECDEBITCYCLE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIRECTDEBITSCHEDULEPLAN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DONATIONAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFECTIVEDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPIREDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXTRACTDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IDENTITYBANKINGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTEXTRACTDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:NEXTEXTRACTDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRANSMITDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null
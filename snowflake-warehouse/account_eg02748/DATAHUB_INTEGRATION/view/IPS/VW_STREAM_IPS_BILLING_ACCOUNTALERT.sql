CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_ACCOUNTALERT AS SELECT
                        src:ACCOUNTALERTKEY::integer AS ACCOUNTALERTKEY, 
                        src:ACCOUNTDELINQUENCYKEY::integer AS ACCOUNTDELINQUENCYKEY, 
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCTALERTTYPE::varchar AS ACCTALERTTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ALERTDEFNKEY::integer AS ALERTDEFNKEY, 
                        src:APPENDEDALERTTEXT::varchar AS APPENDEDALERTTEXT, 
                        src:BANKRUPTCYKEY::integer AS BANKRUPTCYKEY, 
                        src:COMMENTS::varchar AS COMMENTS, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DELINQUENCYMSALERTKEY::integer AS DELINQUENCYMSALERTKEY, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:INITIATEDBY::varchar AS INITIATEDBY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:REASON::varchar AS REASON, 
                        src:RESOLUTION::varchar AS RESOLUTION, 
                        src:RESOLVEDBY::varchar AS RESOLVEDBY, 
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
                                        
                src:ACCOUNTALERTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ACCOUNTALERT as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTALERTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTDELINQUENCYKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALERTDEFNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BANKRUPTCYKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DELINQUENCYMSALERTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null
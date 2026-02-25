CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_FIXCHGSU AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BILLEDTHROUGHDATE1::datetime AS BILLEDTHROUGHDATE1, 
                        src:BILLEDTHROUGHDATE10::datetime AS BILLEDTHROUGHDATE10, 
                        src:BILLEDTHROUGHDATE11::datetime AS BILLEDTHROUGHDATE11, 
                        src:BILLEDTHROUGHDATE12::datetime AS BILLEDTHROUGHDATE12, 
                        src:BILLEDTHROUGHDATE2::datetime AS BILLEDTHROUGHDATE2, 
                        src:BILLEDTHROUGHDATE3::datetime AS BILLEDTHROUGHDATE3, 
                        src:BILLEDTHROUGHDATE4::datetime AS BILLEDTHROUGHDATE4, 
                        src:BILLEDTHROUGHDATE5::datetime AS BILLEDTHROUGHDATE5, 
                        src:BILLEDTHROUGHDATE6::datetime AS BILLEDTHROUGHDATE6, 
                        src:BILLEDTHROUGHDATE7::datetime AS BILLEDTHROUGHDATE7, 
                        src:BILLEDTHROUGHDATE8::datetime AS BILLEDTHROUGHDATE8, 
                        src:BILLEDTHROUGHDATE9::datetime AS BILLEDTHROUGHDATE9, 
                        src:DELETED::boolean AS DELETED, 
                        src:FIXCHGSUKEY::integer AS FIXCHGSUKEY, 
                        src:FREQUENCY::varchar AS FREQUENCY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
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
                                        
                src:FIXCHGSUKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_FIXCHGSU as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLEDTHROUGHDATE1), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLEDTHROUGHDATE10), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLEDTHROUGHDATE11), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLEDTHROUGHDATE12), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLEDTHROUGHDATE2), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLEDTHROUGHDATE3), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLEDTHROUGHDATE4), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLEDTHROUGHDATE5), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLEDTHROUGHDATE6), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLEDTHROUGHDATE7), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLEDTHROUGHDATE8), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BILLEDTHROUGHDATE9), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FIXCHGSUKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null
CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLBILLING_ACCOUNTEXTN AS SELECT
                        src:ACCOUNTEXTNKEY::integer AS ACCOUNTEXTNKEY, 
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACENTITY::varchar AS ACENTITY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:DELETED::boolean AS DELETED, 
                        src:DELIVERYMETHOD::varchar AS DELIVERYMETHOD, 
                        src:HANSENAUTHORITY::varchar AS HANSENAUTHORITY, 
                        src:IGCAPPLICATION::varchar AS IGCAPPLICATION, 
                        src:IGCBASELINE::numeric(38, 10) AS IGCBASELINE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OWNEREMAIL::varchar AS OWNEREMAIL, 
                        src:PIVOTAL::varchar AS PIVOTAL, 
                        src:PMEMAIL::varchar AS PMEMAIL, 
                        src:REMOVEHANSENAUTH::varchar AS REMOVEHANSENAUTH, 
                        src:STARTDATE::datetime AS STARTDATE, 
                        src:STOPDATE::datetime AS STOPDATE, 
                        src:TENANTEMAIL::varchar AS TENANTEMAIL, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WEBEMAILCC::varchar AS WEBEMAILCC, 
                        src:WEBEMAILTO::varchar AS WEBEMAILTO, 
                        src:XERO::varchar AS XERO, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:ACCOUNTEXTNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLBILLING_ACCOUNTEXTN as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTEXTNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IGCBASELINE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STARTDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STOPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null
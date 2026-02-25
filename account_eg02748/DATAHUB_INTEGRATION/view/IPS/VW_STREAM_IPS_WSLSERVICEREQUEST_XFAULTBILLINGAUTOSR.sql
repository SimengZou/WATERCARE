CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLSERVICEREQUEST_XFAULTBILLINGAUTOSR AS SELECT
                        src:ACTIVITYCODE::varchar AS ACTIVITYCODE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESS::varchar AS ADDRESS, 
                        src:DELETED::boolean AS DELETED, 
                        src:DISCONNECTIONDATE::varchar AS DISCONNECTIONDATE, 
                        src:DISCONNECTIONTYPE::varchar AS DISCONNECTIONTYPE, 
                        src:IPSCOMPKEY::integer AS IPSCOMPKEY, 
                        src:LEAKCUSTOMERSIDE::varchar AS LEAKCUSTOMERSIDE, 
                        src:LEAKWATERCARESIDE::varchar AS LEAKWATERCARESIDE, 
                        src:LOCATIONDESCRIPTION::varchar AS LOCATIONDESCRIPTION, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEWMETERID::varchar AS NEWMETERID, 
                        src:NEWMETERINSTALLDATE::varchar AS NEWMETERINSTALLDATE, 
                        src:NEWMETERREADING::varchar AS NEWMETERREADING, 
                        src:OLDMETERID::varchar AS OLDMETERID, 
                        src:OLDMETERREADING::varchar AS OLDMETERREADING, 
                        src:ORIGINATEDSR::integer AS ORIGINATEDSR, 
                        src:SRCREATED::varchar AS SRCREATED, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WORKORDER::varchar AS WORKORDER, 
                        src:XFAULTBILLINGAUTOSRKEY::integer AS XFAULTBILLINGAUTOSRKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XFAULTBILLINGAUTOSRKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLSERVICEREQUEST_XFAULTBILLINGAUTOSR as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IPSCOMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORIGINATEDSR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XFAULTBILLINGAUTOSRKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null
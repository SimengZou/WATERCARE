CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLRESOURCES_XADDRESSGIS AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDREXPIRED::varchar AS ADDREXPIRED, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:ARBORISTREQUIRED::varchar AS ARBORISTREQUIRED, 
                        src:CONNECTIONTYPE::varchar AS CONNECTIONTYPE, 
                        src:CRITICALLINESAVAILABLE::varchar AS CRITICALLINESAVAILABLE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:HASSPECIALIGCAGREEMENT::varchar AS HASSPECIALIGCAGREEMENT, 
                        src:IGCZONE::varchar AS IGCZONE, 
                        src:INCOMBINEDAREA::varchar AS INCOMBINEDAREA, 
                        src:INPRESSUREDZONE::varchar AS INPRESSUREDZONE, 
                        src:INREDZONE::varchar AS INREDZONE, 
                        src:LEVELOFROAD::varchar AS LEVELOFROAD, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:SERVICEAVAILABLE::varchar AS SERVICEAVAILABLE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WASTEWATERPIPELEVEL::varchar AS WASTEWATERPIPELEVEL, 
                        src:XADDRESSGISKEY::integer AS XADDRESSGISKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XADDRESSGISKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLRESOURCES_XADDRESSGIS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XADDRESSGISKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null
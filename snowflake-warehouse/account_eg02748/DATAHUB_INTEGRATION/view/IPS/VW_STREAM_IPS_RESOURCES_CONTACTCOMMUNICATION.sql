CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_CONTACTCOMMUNICATION AS SELECT
                        src:ACTIVITYKEY::integer AS ACTIVITYKEY, 
                        src:ACTIVITYTYPE::varchar AS ACTIVITYTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CONTACTCOMMBODYKEY::integer AS CONTACTCOMMBODYKEY, 
                        src:CONTACTCOMMUNICATIONKEY::integer AS CONTACTCOMMUNICATIONKEY, 
                        src:CONTACTKEY::integer AS CONTACTKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DELIVERYRESULT::integer AS DELIVERYRESULT, 
                        src:ISHTML::varchar AS ISHTML, 
                        src:MESSAGE::varchar AS MESSAGE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOTIFICATIONMETHOD::varchar AS NOTIFICATIONMETHOD, 
                        src:NOTIFICATIONTYPE::varchar AS NOTIFICATIONTYPE, 
                        src:RECIPIENT::varchar AS RECIPIENT, 
                        src:SENDER::varchar AS SENDER, 
                        src:TEMPLATEID::varchar AS TEMPLATEID, 
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
                                        
                src:CONTACTCOMMUNICATIONKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_RESOURCES_CONTACTCOMMUNICATION as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACTIVITYKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONTACTCOMMBODYKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONTACTCOMMUNICATIONKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONTACTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DELIVERYRESULT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null
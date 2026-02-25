CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRPLAN_XPLANDPACTIONLISTGD AS SELECT
                        src:ACTIONNOTES::varchar AS ACTIONNOTES, 
                        src:ACTIONOWNER::varchar AS ACTIONOWNER, 
                        src:ACTIONSTATUS::varchar AS ACTIONSTATUS, 
                        src:ACTIONTYPE::varchar AS ACTIONTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSIGNEDT1::varchar AS ASSIGNEDT1, 
                        src:ASSIGNEDTO::varchar AS ASSIGNEDTO, 
                        src:COMPLETEDDATE::datetime AS COMPLETEDDATE, 
                        src:CREATEDATE::datetime AS CREATEDATE, 
                        src:DELETED::boolean AS DELETED, 
                        src:DETAILS::varchar AS DETAILS, 
                        src:DUEDATE::datetime AS DUEDATE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:RESPONSIBLE::varchar AS RESPONSIBLE, 
                        src:REVIEWID::integer AS REVIEWID, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XPLANDPACTIONLISTGDKEY::integer AS XPLANDPACTIONLISTGDKEY, 
                        src:XPLANDPACTIONLISTKEY::integer AS XPLANDPACTIONLISTKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XPLANDPACTIONLISTGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRPLAN_XPLANDPACTIONLISTGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:COMPLETEDDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CREATEDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DUEDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVIEWID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XPLANDPACTIONLISTGDKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XPLANDPACTIONLISTKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null
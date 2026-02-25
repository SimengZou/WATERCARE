CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRPROJ_XPROJCOMPDP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROJAPPLDTLKEY::integer AS APPROJAPPLDTLKEY, 
                        src:COAASBUILTATTACHED::varchar AS COAASBUILTATTACHED, 
                        src:CS3ATTACHED::varchar AS CS3ATTACHED, 
                        src:CS4ATTACHED::varchar AS CS4ATTACHED, 
                        src:CS4DAILYLOGATTACHED::varchar AS CS4DAILYLOGATTACHED, 
                        src:CS4SCHEDULEATTACHED::varchar AS CS4SCHEDULEATTACHED, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DRAFTASBUILTATTACHED::varchar AS DRAFTASBUILTATTACHED, 
                        src:DRAFTASBUILTINWALKOVER::varchar AS DRAFTASBUILTINWALKOVER, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:REQCOA::varchar AS REQCOA, 
                        src:REQDATEWALKOVER::datetime AS REQDATEWALKOVER, 
                        src:REQWALKOVER::varchar AS REQWALKOVER, 
                        src:SCHEDULEOFCOSTATTACHED::varchar AS SCHEDULEOFCOSTATTACHED, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XPROJCOMPDPKEY::integer AS XPROJCOMPDPKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XPROJCOMPDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRPROJ_XPROJCOMPDP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APPROJAPPLDTLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:REQDATEWALKOVER), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XPROJCOMPDPKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null
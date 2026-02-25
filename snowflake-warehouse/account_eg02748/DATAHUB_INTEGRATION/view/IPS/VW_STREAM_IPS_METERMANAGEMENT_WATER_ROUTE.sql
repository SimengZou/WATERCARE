CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_METERMANAGEMENT_WATER_ROUTE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AVERAGECONSUMPTION::numeric(38, 10) AS AVERAGECONSUMPTION, 
                        src:BILLRTFLAG::varchar AS BILLRTFLAG, 
                        src:DELETED::boolean AS DELETED, 
                        src:LASTDTTM::datetime AS LASTDTTM, 
                        src:LASTREADINGEXSCHDKEY::integer AS LASTREADINGEXSCHDKEY, 
                        src:LASTREADINGIMSCHDKEY::integer AS LASTREADINGIMSCHDKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEXTDTTM::datetime AS NEXTDTTM, 
                        src:OUTFORACT::varchar AS OUTFORACT, 
                        src:READINGCYCLE::varchar AS READINGCYCLE, 
                        src:ROUTEDESC::varchar AS ROUTEDESC, 
                        src:ROUTEID::varchar AS ROUTEID, 
                        src:ROUTEKEY::integer AS ROUTEKEY, 
                        src:ROUTETIME::varchar AS ROUTETIME, 
                        src:TIMEINT::integer AS TIMEINT, 
                        src:TIMEUNIT::varchar AS TIMEUNIT, 
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
                                        
                src:ROUTEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_ROUTE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AVERAGECONSUMPTION), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LASTREADINGEXSCHDKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LASTREADINGIMSCHDKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:NEXTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROUTEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TIMEINT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null
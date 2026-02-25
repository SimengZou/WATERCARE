CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_METERMANAGEMENT_WATER_ROUTE AS SELECT
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
                        src:VARIATION_ID::integer AS VARIATION_ID, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:ROUTEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ROUTEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_METERMANAGEMENT_WATER_ROUTE as strm))
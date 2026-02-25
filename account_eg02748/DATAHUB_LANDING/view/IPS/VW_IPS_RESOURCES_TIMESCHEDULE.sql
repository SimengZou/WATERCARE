CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_RESOURCES_TIMESCHEDULE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DAYFLAGS::integer AS DAYFLAGS, 
                        src:DAYOFMONTH::integer AS DAYOFMONTH, 
                        src:DAYOFWEEK::integer AS DAYOFWEEK, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:MAXDAYSADV::integer AS MAXDAYSADV, 
                        src:MIGSRCKEY::integer AS MIGSRCKEY, 
                        src:MINDAYSADV::integer AS MINDAYSADV, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MONTHOFYEAR::integer AS MONTHOFYEAR, 
                        src:NEXTSCHEDDATE::datetime AS NEXTSCHEDDATE, 
                        src:REPEATHOURS::integer AS REPEATHOURS, 
                        src:REPEATMINUTES::integer AS REPEATMINUTES, 
                        src:REPEATUNTILHOURS::integer AS REPEATUNTILHOURS, 
                        src:REPEATUNTILMIN::integer AS REPEATUNTILMIN, 
                        src:REPEATUNTILTIME::datetime AS REPEATUNTILTIME, 
                        src:SCHEDTYPE::integer AS SCHEDTYPE, 
                        src:SKIPDAYS::integer AS SKIPDAYS, 
                        src:SKIPMONTHS::integer AS SKIPMONTHS, 
                        src:SKIPWEEKS::integer AS SKIPWEEKS, 
                        src:STARTDATE::datetime AS STARTDATE, 
                        src:TASKREPEAT::varchar AS TASKREPEAT, 
                        src:TIMESCHEDKEY::integer AS TIMESCHEDKEY, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WEEKFLAGS::integer AS WEEKFLAGS, 
                        src:WEEKOFMONTH::integer AS WEEKOFMONTH, 
                        src:YEARLYDATE::datetime AS YEARLYDATE, 
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
    
                        
                src:TIMESCHEDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:TIMESCHEDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_RESOURCES_TIMESCHEDULE as strm))
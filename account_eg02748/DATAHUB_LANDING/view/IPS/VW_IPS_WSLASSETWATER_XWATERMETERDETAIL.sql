CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLASSETWATER_XWATERMETERDETAIL AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:DAILYAVERAGE::numeric(38, 10) AS DAILYAVERAGE, 
                        src:DELETED::boolean AS DELETED, 
                        src:ISSMARTLOGGER::varchar AS ISSMARTLOGGER, 
                        src:LASTREADERCODE::varchar AS LASTREADERCODE, 
                        src:LASTREADERCODEDATE::datetime AS LASTREADERCODEDATE, 
                        src:LASTVALIDATEDDATETIME::datetime AS LASTVALIDATEDDATETIME, 
                        src:LASTYEARDAILYAVERAGE::numeric(38, 10) AS LASTYEARDAILYAVERAGE, 
                        src:METERZEROCODE::varchar AS METERZEROCODE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PHONENUMBER::varchar AS PHONENUMBER, 
                        src:READERCODECOUNT::integer AS READERCODECOUNT, 
                        src:READINGROLLOVER::varchar AS READINGROLLOVER, 
                        src:RECALCDAILYAVG::varchar AS RECALCDAILYAVG, 
                        src:SMS::varchar AS SMS, 
                        src:VALIDATEDBY::varchar AS VALIDATEDBY, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XWATERMETERDETAILKEY::integer AS XWATERMETERDETAILKEY, 
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
    
                        
                src:XWATERMETERDETAILKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XWATERMETERDETAILKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLASSETWATER_XWATERMETERDETAIL as strm))
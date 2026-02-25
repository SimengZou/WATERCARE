CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_RESOURCES_RATETABLE_RATETBL AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CYCLEDAYS::integer AS CYCLEDAYS, 
                        src:DELETED::boolean AS DELETED, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:FINALCYCLEDAYS::integer AS FINALCYCLEDAYS, 
                        src:FORMULA::integer AS FORMULA, 
                        src:FORMULALAST::varchar AS FORMULALAST, 
                        src:ISPRORATED::varchar AS ISPRORATED, 
                        src:MAXIMUMCHARGE::numeric(38, 10) AS MAXIMUMCHARGE, 
                        src:MINIMUMCHARGE::numeric(38, 10) AS MINIMUMCHARGE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEWEFFECTIVEDATE::datetime AS NEWEFFECTIVEDATE, 
                        src:NUMBEROFDECIMALS::integer AS NUMBEROFDECIMALS, 
                        src:PERCENTINCREASE::numeric(38, 10) AS PERCENTINCREASE, 
                        src:RATECODEKEY::integer AS RATECODEKEY, 
                        src:RATETBLKEY::integer AS RATETBLKEY, 
                        src:USEFORPRORATING::integer AS USEFORPRORATING, 
                        src:USEOVERAGE::varchar AS USEOVERAGE, 
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
    
                        
                src:RATETBLKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:RATETBLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_RESOURCES_RATETABLE_RATETBL as strm))
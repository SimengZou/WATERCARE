CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_RESOURCES_RATETABLE_CALCULATIONTABLE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CALCULATIONTABLEKEY::integer AS CALCULATIONTABLEKEY, 
                        src:CYCLEDAYS::integer AS CYCLEDAYS, 
                        src:DELETED::boolean AS DELETED, 
                        src:FORMULAINPUT::numeric(38, 10) AS FORMULAINPUT, 
                        src:FORMULALAST::varchar AS FORMULALAST, 
                        src:FORMULANAME::varchar AS FORMULANAME, 
                        src:FORMULAOUTPUT::numeric(38, 10) AS FORMULAOUTPUT, 
                        src:FORMULAUSED::varchar AS FORMULAUSED, 
                        src:MAXIMUMCHARGE::numeric(38, 10) AS MAXIMUMCHARGE, 
                        src:MAXIMUMUSED::varchar AS MAXIMUMUSED, 
                        src:MINIMUMCHARGE::numeric(38, 10) AS MINIMUMCHARGE, 
                        src:MINIMUMUSED::varchar AS MINIMUMUSED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PRORATED::varchar AS PRORATED, 
                        src:RATETABLESETUPKEY::integer AS RATETABLESETUPKEY, 
                        src:USAGEDAYS::integer AS USAGEDAYS, 
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
    
                        
                src:CALCULATIONTABLEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CALCULATIONTABLEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_RESOURCES_RATETABLE_CALCULATIONTABLE as strm))
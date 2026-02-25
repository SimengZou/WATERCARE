CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_PORTAL_SCENARIO AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CONFIGURATIONID::varchar AS CONFIGURATIONID, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:LASTSAVE::datetime AS LASTSAVE, 
                        src:MEMBERSHIP::integer AS MEMBERSHIP, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NAME::varchar AS NAME, 
                        src:PROCESSID::varchar AS PROCESSID, 
                        src:PROJECTAPPLICATION::integer AS PROJECTAPPLICATION, 
                        src:REQRECCOUNT::integer AS REQRECCOUNT, 
                        src:SCENARIOKEY::integer AS SCENARIOKEY, 
                        src:STATUS::varchar AS STATUS, 
                        src:SUBRECCOUNT::integer AS SUBRECCOUNT, 
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
    
                        
                src:SCENARIOKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:SCENARIOKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_PORTAL_SCENARIO as strm))
CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5EVENTCOST AS SELECT
                        src:EVO_COSTCALCULATED::varchar AS EVO_COSTCALCULATED, 
                        src:EVO_DIRECTMATERIAL::numeric(38, 10) AS EVO_DIRECTMATERIAL, 
                        src:EVO_EVENT::varchar AS EVO_EVENT, 
                        src:EVO_HIREDLABOR::numeric(38, 10) AS EVO_HIREDLABOR, 
                        src:EVO_HOURS::numeric(38, 10) AS EVO_HOURS, 
                        src:EVO_LABOR::numeric(38, 10) AS EVO_LABOR, 
                        src:EVO_LASTSAVED::datetime AS EVO_LASTSAVED, 
                        src:EVO_MATERIAL::numeric(38, 10) AS EVO_MATERIAL, 
                        src:EVO_OWNLABOR::numeric(38, 10) AS EVO_OWNLABOR, 
                        src:EVO_RECALCCOST::varchar AS EVO_RECALCCOST, 
                        src:EVO_SERVICESLABOR::numeric(38, 10) AS EVO_SERVICESLABOR, 
                        src:EVO_STOCKMATERIAL::numeric(38, 10) AS EVO_STOCKMATERIAL, 
                        src:EVO_TOOL::numeric(38, 10) AS EVO_TOOL, 
                        src:EVO_TOTAL::numeric(38, 10) AS EVO_TOTAL, 
                        src:EVO_UPDATED::datetime AS EVO_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
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
    
                        
                src:EVO_EVENT,
            src:EVO_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:EVO_EVENT  ORDER BY 
            src:EVO_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5EVENTCOST as strm))
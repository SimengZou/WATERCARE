CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5FUNCTIONTABGRIDS AS SELECT
                        src:FTG_FUNCTION::varchar AS FTG_FUNCTION, 
                        src:FTG_GRIDID::numeric(38, 10) AS FTG_GRIDID, 
                        src:FTG_LASTSAVED::datetime AS FTG_LASTSAVED, 
                        src:FTG_TAB::varchar AS FTG_TAB, 
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
    
                        
                src:FTG_FUNCTION,
                src:FTG_GRIDID,
                src:FTG_TAB,
            src:FTG_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:FTG_FUNCTION,
                src:FTG_GRIDID,
                src:FTG_TAB  ORDER BY 
            src:FTG_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5FUNCTIONTABGRIDS as strm))
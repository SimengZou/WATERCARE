CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5EVTSYSTEMS AS SELECT
                        src:ESY_EVENT::varchar AS ESY_EVENT, 
                        src:ESY_LASTSAVED::datetime AS ESY_LASTSAVED, 
                        src:ESY_SYSTEM::varchar AS ESY_SYSTEM, 
                        src:ESY_SYSTEM_ORG::varchar AS ESY_SYSTEM_ORG, 
                        src:ESY_UPDATECOUNT::numeric(38, 10) AS ESY_UPDATECOUNT, 
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
    
                        
                src:ESY_EVENT,
                src:ESY_SYSTEM,
                src:ESY_SYSTEM_ORG,
            src:ESY_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ESY_EVENT,
                src:ESY_SYSTEM,
                src:ESY_SYSTEM_ORG  ORDER BY 
            src:ESY_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5EVTSYSTEMS as strm))
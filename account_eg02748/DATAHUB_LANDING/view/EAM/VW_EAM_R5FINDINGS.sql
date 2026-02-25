CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5FINDINGS AS SELECT
                        src:FND_CLASS::varchar AS FND_CLASS, 
                        src:FND_CLASS_ORG::varchar AS FND_CLASS_ORG, 
                        src:FND_CODE::varchar AS FND_CODE, 
                        src:FND_CREATED::datetime AS FND_CREATED, 
                        src:FND_DESC::varchar AS FND_DESC, 
                        src:FND_GEN::varchar AS FND_GEN, 
                        src:FND_LASTSAVED::datetime AS FND_LASTSAVED, 
                        src:FND_UPDATECOUNT::numeric(38, 10) AS FND_UPDATECOUNT, 
                        src:FND_UPDATED::datetime AS FND_UPDATED, 
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
    
                        
                src:FND_CODE,
            src:FND_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:FND_CODE  ORDER BY 
            src:FND_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5FINDINGS as strm))
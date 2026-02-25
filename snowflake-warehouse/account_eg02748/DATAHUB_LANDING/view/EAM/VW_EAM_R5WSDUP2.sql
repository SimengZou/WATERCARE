CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5WSDUP2 AS SELECT
                        src:WD2_CODE::varchar AS WD2_CODE, 
                        src:WD2_DESC::varchar AS WD2_DESC, 
                        src:WD2_LASTSAVED::datetime AS WD2_LASTSAVED, 
                        src:WD2_TIME::datetime AS WD2_TIME, 
                        src:WD2_TYPE::varchar AS WD2_TYPE, 
                        src:WD2_UPDATECOUNT::numeric(38, 10) AS WD2_UPDATECOUNT, 
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
    
                        
                src:WD2_CODE,
            src:WD2_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:WD2_CODE  ORDER BY 
            src:WD2_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5WSDUP2 as strm))
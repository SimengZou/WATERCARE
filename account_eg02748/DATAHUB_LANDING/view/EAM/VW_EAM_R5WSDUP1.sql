CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5WSDUP1 AS SELECT
                        src:WD1_CODE::varchar AS WD1_CODE, 
                        src:WD1_DESC::varchar AS WD1_DESC, 
                        src:WD1_LASTSAVED::datetime AS WD1_LASTSAVED, 
                        src:WD1_TIME::datetime AS WD1_TIME, 
                        src:WD1_TYPE::varchar AS WD1_TYPE, 
                        src:WD1_UPDATECOUNT::numeric(38, 10) AS WD1_UPDATECOUNT, 
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
    
                        
                src:WD1_CODE,
            src:WD1_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:WD1_CODE  ORDER BY 
            src:WD1_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5WSDUP1 as strm))
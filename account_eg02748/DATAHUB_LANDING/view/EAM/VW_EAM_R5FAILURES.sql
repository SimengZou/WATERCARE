CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5FAILURES AS SELECT
                        src:FAL_CODE::varchar AS FAL_CODE, 
                        src:FAL_CREATED::datetime AS FAL_CREATED, 
                        src:FAL_DESC::varchar AS FAL_DESC, 
                        src:FAL_ENABLEWORKORDERS::varchar AS FAL_ENABLEWORKORDERS, 
                        src:FAL_GEN::varchar AS FAL_GEN, 
                        src:FAL_GROUP::varchar AS FAL_GROUP, 
                        src:FAL_LASTSAVED::datetime AS FAL_LASTSAVED, 
                        src:FAL_NOTUSED::varchar AS FAL_NOTUSED, 
                        src:FAL_PARTFAILURE::varchar AS FAL_PARTFAILURE, 
                        src:FAL_UPDATECOUNT::numeric(38, 10) AS FAL_UPDATECOUNT, 
                        src:FAL_UPDATED::datetime AS FAL_UPDATED, 
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
    
                        
                src:FAL_CODE,
            src:FAL_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:FAL_CODE  ORDER BY 
            src:FAL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5FAILURES as strm))
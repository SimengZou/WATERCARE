CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5CAUSES AS SELECT
                        src:CAU_CODE::varchar AS CAU_CODE, 
                        src:CAU_CREATED::datetime AS CAU_CREATED, 
                        src:CAU_DESC::varchar AS CAU_DESC, 
                        src:CAU_ENABLEWORKORDERS::varchar AS CAU_ENABLEWORKORDERS, 
                        src:CAU_GEN::varchar AS CAU_GEN, 
                        src:CAU_GROUP::varchar AS CAU_GROUP, 
                        src:CAU_LASTSAVED::datetime AS CAU_LASTSAVED, 
                        src:CAU_NOTUSED::varchar AS CAU_NOTUSED, 
                        src:CAU_PARTFAILURE::varchar AS CAU_PARTFAILURE, 
                        src:CAU_UPDATECOUNT::numeric(38, 10) AS CAU_UPDATECOUNT, 
                        src:CAU_UPDATED::datetime AS CAU_UPDATED, 
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
    
                        
                src:CAU_CODE,
            src:CAU_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CAU_CODE  ORDER BY 
            src:CAU_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5CAUSES as strm))
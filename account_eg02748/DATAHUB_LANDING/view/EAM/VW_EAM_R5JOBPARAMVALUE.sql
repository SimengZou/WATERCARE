CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5JOBPARAMVALUE AS SELECT
                        src:JPV_CODE::varchar AS JPV_CODE, 
                        src:JPV_KEY::varchar AS JPV_KEY, 
                        src:JPV_LASTSAVED::datetime AS JPV_LASTSAVED, 
                        src:JPV_LINE::numeric(38, 10) AS JPV_LINE, 
                        src:JPV_UPDATECOUNT::numeric(38, 10) AS JPV_UPDATECOUNT, 
                        src:JPV_VALUE::varchar AS JPV_VALUE, 
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
    
                        
                src:JPV_CODE,
                src:JPV_LINE,
            src:JPV_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:JPV_CODE,
                src:JPV_LINE  ORDER BY 
            src:JPV_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5JOBPARAMVALUE as strm))
CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5AVAILABILITY AS SELECT
                        src:AVL_DATE::datetime AS AVL_DATE, 
                        src:AVL_HIRHOURS::numeric(38, 10) AS AVL_HIRHOURS, 
                        src:AVL_LASTSAVED::datetime AS AVL_LASTSAVED, 
                        src:AVL_MRC::varchar AS AVL_MRC, 
                        src:AVL_NETHIRED::numeric(38, 10) AS AVL_NETHIRED, 
                        src:AVL_NETHOURS::numeric(38, 10) AS AVL_NETHOURS, 
                        src:AVL_OWNHOURS::numeric(38, 10) AS AVL_OWNHOURS, 
                        src:AVL_SPECIAL::varchar AS AVL_SPECIAL, 
                        src:AVL_TRADE::varchar AS AVL_TRADE, 
                        src:AVL_UPDATECOUNT::numeric(38, 10) AS AVL_UPDATECOUNT, 
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
    
                        
                src:AVL_DATE,
                src:AVL_MRC,
                src:AVL_TRADE,
            src:AVL_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:AVL_DATE,
                src:AVL_MRC,
                src:AVL_TRADE  ORDER BY 
            src:AVL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5AVAILABILITY as strm))
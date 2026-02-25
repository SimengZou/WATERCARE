CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ALERTGENERATEWO AS SELECT
                        src:AGW_ALERT::varchar AS AGW_ALERT, 
                        src:AGW_GENERATETHROUGHFIELDID::numeric(38, 10) AS AGW_GENERATETHROUGHFIELDID, 
                        src:AGW_LASTSAVED::datetime AS AGW_LASTSAVED, 
                        src:AGW_TYPE::varchar AS AGW_TYPE, 
                        src:AGW_UPDATECOUNT::numeric(38, 10) AS AGW_UPDATECOUNT, 
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
    
                        
                src:AGW_ALERT,
            src:AGW_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:AGW_ALERT  ORDER BY 
            src:AGW_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ALERTGENERATEWO as strm))
CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ALERTMAIL AS SELECT
                        src:ALM_ALERT::varchar AS ALM_ALERT, 
                        src:ALM_DELAY::numeric(38, 10) AS ALM_DELAY, 
                        src:ALM_DELAYUOM::varchar AS ALM_DELAYUOM, 
                        src:ALM_LASTSAVED::datetime AS ALM_LASTSAVED, 
                        src:ALM_TEMPLATE::varchar AS ALM_TEMPLATE, 
                        src:ALM_UPDATECOUNT::numeric(38, 10) AS ALM_UPDATECOUNT, 
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
    
                        
                src:ALM_ALERT,
                src:ALM_TEMPLATE,
            src:ALM_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ALM_ALERT,
                src:ALM_TEMPLATE  ORDER BY 
            src:ALM_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ALERTMAIL as strm))
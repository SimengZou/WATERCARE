CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5USERDEFINEDFIELDDEPENDENCIES AS SELECT
                        src:UFD_GRIDCACHE::varchar AS UFD_GRIDCACHE, 
                        src:UFD_LASTSAVED::datetime AS UFD_LASTSAVED, 
                        src:UFD_LAYOUTCACHE::varchar AS UFD_LAYOUTCACHE, 
                        src:UFD_PAGENAME::varchar AS UFD_PAGENAME, 
                        src:UFD_RENTITY::varchar AS UFD_RENTITY, 
                        src:UFD_UPDATECOUNT::numeric(38, 10) AS UFD_UPDATECOUNT, 
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
    
                        
                src:UFD_PAGENAME,
                src:UFD_RENTITY,
            src:UFD_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:UFD_PAGENAME,
                src:UFD_RENTITY  ORDER BY 
            src:UFD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5USERDEFINEDFIELDDEPENDENCIES as strm))
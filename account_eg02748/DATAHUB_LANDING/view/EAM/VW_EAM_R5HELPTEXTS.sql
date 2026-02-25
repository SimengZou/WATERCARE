CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5HELPTEXTS AS SELECT
                        src:HEL_CHANGED::varchar AS HEL_CHANGED, 
                        src:HEL_DRILLDOWN::varchar AS HEL_DRILLDOWN, 
                        src:HEL_FUNCTION::varchar AS HEL_FUNCTION, 
                        src:HEL_ITEM::varchar AS HEL_ITEM, 
                        src:HEL_LANG::varchar AS HEL_LANG, 
                        src:HEL_LASTSAVED::datetime AS HEL_LASTSAVED, 
                        src:HEL_POOL::varchar AS HEL_POOL, 
                        src:HEL_TEXT::varchar AS HEL_TEXT, 
                        src:HEL_UPDATECOUNT::numeric(38, 10) AS HEL_UPDATECOUNT, 
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
    
                        
                src:HEL_FUNCTION,
                src:HEL_ITEM,
                src:HEL_LANG,
            src:HEL_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:HEL_FUNCTION,
                src:HEL_ITEM,
                src:HEL_LANG  ORDER BY 
            src:HEL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5HELPTEXTS as strm))
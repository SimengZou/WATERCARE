CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5REMEMBER AS SELECT
                        src:RMB_FUNCTION::varchar AS RMB_FUNCTION, 
                        src:RMB_ITEM::varchar AS RMB_ITEM, 
                        src:RMB_ITEM2::varchar AS RMB_ITEM2, 
                        src:RMB_LASTSAVED::datetime AS RMB_LASTSAVED, 
                        src:RMB_RENTITY::varchar AS RMB_RENTITY, 
                        src:RMB_UPDATECOUNT::numeric(38, 10) AS RMB_UPDATECOUNT, 
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
    
                        
                src:RMB_FUNCTION,
                src:RMB_ITEM,
                src:RMB_RENTITY,
            src:RMB_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:RMB_FUNCTION,
                src:RMB_ITEM,
                src:RMB_RENTITY  ORDER BY 
            src:RMB_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5REMEMBER as strm))
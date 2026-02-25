CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5DRIVERCTRL AS SELECT
                        src:DRV_ACTION::varchar AS DRV_ACTION, 
                        src:DRV_CODE::varchar AS DRV_CODE, 
                        src:DRV_FREQUENCY::numeric(38, 10) AS DRV_FREQUENCY, 
                        src:DRV_LASTRAN::datetime AS DRV_LASTRAN, 
                        src:DRV_LASTSAVED::datetime AS DRV_LASTSAVED, 
                        src:DRV_NEXTRUN::datetime AS DRV_NEXTRUN, 
                        src:DRV_QUEUE::varchar AS DRV_QUEUE, 
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
    
                        
                src:DRV_CODE,
                src:DRV_QUEUE,
            src:DRV_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:DRV_CODE,
                src:DRV_QUEUE  ORDER BY 
            src:DRV_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5DRIVERCTRL as strm))
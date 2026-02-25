CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5SHFDAYS AS SELECT
                        src:SHD_DAY::numeric(38, 10) AS SHD_DAY, 
                        src:SHD_HOURS::numeric(38, 10) AS SHD_HOURS, 
                        src:SHD_LASTSAVED::datetime AS SHD_LASTSAVED, 
                        src:SHD_SHIFT::varchar AS SHD_SHIFT, 
                        src:SHD_TIME::numeric(38, 10) AS SHD_TIME, 
                        src:SHD_UPDATECOUNT::numeric(38, 10) AS SHD_UPDATECOUNT, 
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
    
                        
                src:SHD_DAY,
                src:SHD_SHIFT,
                src:SHD_TIME,
            src:SHD_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:SHD_DAY,
                src:SHD_SHIFT,
                src:SHD_TIME  ORDER BY 
            src:SHD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5SHFDAYS as strm))
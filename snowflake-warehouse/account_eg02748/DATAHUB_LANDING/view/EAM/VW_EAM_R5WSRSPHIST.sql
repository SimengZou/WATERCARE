CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5WSRSPHIST AS SELECT
                        src:WSR_ADDRESS::varchar AS WSR_ADDRESS, 
                        src:WSR_DATAAREA::numeric(38, 10) AS WSR_DATAAREA, 
                        src:WSR_LASTSAVED::datetime AS WSR_LASTSAVED, 
                        src:WSR_MESSAGE::varchar AS WSR_MESSAGE, 
                        src:WSR_PROCESS::varchar AS WSR_PROCESS, 
                        src:WSR_RSTATUS::varchar AS WSR_RSTATUS, 
                        src:WSR_STATUS::varchar AS WSR_STATUS, 
                        src:WSR_STATUS_MESSAGE::varchar AS WSR_STATUS_MESSAGE, 
                        src:WSR_TIME::datetime AS WSR_TIME, 
                        src:WSR_UPDATECOUNT::numeric(38, 10) AS WSR_UPDATECOUNT, 
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
    
                        
                src:WSR_MESSAGE,
                src:WSR_PROCESS,
            src:WSR_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:WSR_MESSAGE,
                src:WSR_PROCESS  ORDER BY 
            src:WSR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5WSRSPHIST as strm))
CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5WSREQHIST AS SELECT
                        src:WSQ_DATAAREA::numeric(38, 10) AS WSQ_DATAAREA, 
                        src:WSQ_LASTSAVED::datetime AS WSQ_LASTSAVED, 
                        src:WSQ_MESSAGE::varchar AS WSQ_MESSAGE, 
                        src:WSQ_PROCESS::varchar AS WSQ_PROCESS, 
                        src:WSQ_RSTATUS::varchar AS WSQ_RSTATUS, 
                        src:WSQ_STATUS::varchar AS WSQ_STATUS, 
                        src:WSQ_STATUS_MESSAGE::varchar AS WSQ_STATUS_MESSAGE, 
                        src:WSQ_TIME::datetime AS WSQ_TIME, 
                        src:WSQ_UPDATECOUNT::numeric(38, 10) AS WSQ_UPDATECOUNT, 
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
    
                        
                src:WSQ_MESSAGE,
                src:WSQ_PROCESS,
            src:WSQ_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:WSQ_MESSAGE,
                src:WSQ_PROCESS  ORDER BY 
            src:WSQ_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5WSREQHIST as strm))
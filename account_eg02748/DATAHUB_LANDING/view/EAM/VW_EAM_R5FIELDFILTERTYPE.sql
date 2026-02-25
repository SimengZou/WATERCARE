CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5FIELDFILTERTYPE AS SELECT
                        src:FFT_DEFAULTSCREENTYPE::varchar AS FFT_DEFAULTSCREENTYPE, 
                        src:FFT_FUNCTION::varchar AS FFT_FUNCTION, 
                        src:FFT_LASTSAVED::datetime AS FFT_LASTSAVED, 
                        src:FFT_RTYPE::varchar AS FFT_RTYPE, 
                        src:FFT_TYPE::varchar AS FFT_TYPE, 
                        src:FFT_UPDATECOUNT::numeric(38, 10) AS FFT_UPDATECOUNT, 
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
    
                        
                src:FFT_FUNCTION,
                src:FFT_RTYPE,
                src:FFT_TYPE,
            src:FFT_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:FFT_FUNCTION,
                src:FFT_RTYPE,
                src:FFT_TYPE  ORDER BY 
            src:FFT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5FIELDFILTERTYPE as strm))
CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5BOILERTEXTS AS SELECT
                        src:BOT_ADLEN::numeric(38, 10) AS BOT_ADLEN, 
                        src:BOT_CHANGED::varchar AS BOT_CHANGED, 
                        src:BOT_CLONED::varchar AS BOT_CLONED, 
                        src:BOT_CREATED::datetime AS BOT_CREATED, 
                        src:BOT_DEST::varchar AS BOT_DEST, 
                        src:BOT_DISPLAY::varchar AS BOT_DISPLAY, 
                        src:BOT_FLD1::varchar AS BOT_FLD1, 
                        src:BOT_FLD2::varchar AS BOT_FLD2, 
                        src:BOT_FUNCTION::varchar AS BOT_FUNCTION, 
                        src:BOT_LANG::varchar AS BOT_LANG, 
                        src:BOT_LASTSAVED::datetime AS BOT_LASTSAVED, 
                        src:BOT_LENGTH::numeric(38, 10) AS BOT_LENGTH, 
                        src:BOT_LVCR::numeric(38, 10) AS BOT_LVCR, 
                        src:BOT_NUMBER::numeric(38, 10) AS BOT_NUMBER, 
                        src:BOT_PAGE::varchar AS BOT_PAGE, 
                        src:BOT_POOL::varchar AS BOT_POOL, 
                        src:BOT_PRTP::varchar AS BOT_PRTP, 
                        src:BOT_TEXT::varchar AS BOT_TEXT, 
                        src:BOT_UPDATECOUNT::numeric(38, 10) AS BOT_UPDATECOUNT, 
                        src:BOT_UPDATED::datetime AS BOT_UPDATED, 
                        src:BOT_XPOS::numeric(38, 10) AS BOT_XPOS, 
                        src:BOT_YPOS::numeric(38, 10) AS BOT_YPOS, 
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
    
                        
                src:BOT_FUNCTION,
                src:BOT_LANG,
                src:BOT_NUMBER,
            src:BOT_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:BOT_FUNCTION,
                src:BOT_LANG,
                src:BOT_NUMBER  ORDER BY 
            src:BOT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5BOILERTEXTS as strm))
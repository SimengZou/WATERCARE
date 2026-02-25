CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5BARESC AS SELECT
                        src:BCE_COLUMN::varchar AS BCE_COLUMN, 
                        src:BCE_ESCAPE::varchar AS BCE_ESCAPE, 
                        src:BCE_LASTSAVED::datetime AS BCE_LASTSAVED, 
                        src:BCE_LINE::numeric(38, 10) AS BCE_LINE, 
                        src:BCE_TEXT1::varchar AS BCE_TEXT1, 
                        src:BCE_TEXT2::varchar AS BCE_TEXT2, 
                        src:BCE_UPDATECOUNT::numeric(38, 10) AS BCE_UPDATECOUNT, 
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
    
                        
                src:BCE_COLUMN,
                src:BCE_ESCAPE,
                src:BCE_LINE,
            src:BCE_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:BCE_COLUMN,
                src:BCE_ESCAPE,
                src:BCE_LINE  ORDER BY 
            src:BCE_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5BARESC as strm))
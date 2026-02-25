CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5PASSWORDS AS SELECT
                        src:PWD_LASTSAVED::datetime AS PWD_LASTSAVED, 
                        src:PWD_LASTUSED::datetime AS PWD_LASTUSED, 
                        src:PWD_PASSWORD::varchar AS PWD_PASSWORD, 
                        src:PWD_UPDATECOUNT::numeric(38, 10) AS PWD_UPDATECOUNT, 
                        src:PWD_USER::varchar AS PWD_USER, 
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
    
                        
                src:PWD_LASTUSED,
                src:PWD_PASSWORD,
                src:PWD_USER,
            src:PWD_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PWD_LASTUSED,
                src:PWD_PASSWORD,
                src:PWD_USER  ORDER BY 
            src:PWD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5PASSWORDS as strm))
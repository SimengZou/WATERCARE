CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5SHFPERS AS SELECT
                        src:SHP_END::datetime AS SHP_END, 
                        src:SHP_LASTSAVED::datetime AS SHP_LASTSAVED, 
                        src:SHP_PERSON::varchar AS SHP_PERSON, 
                        src:SHP_SHIFT::varchar AS SHP_SHIFT, 
                        src:SHP_START::datetime AS SHP_START, 
                        src:SHP_UPDATECOUNT::numeric(38, 10) AS SHP_UPDATECOUNT, 
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
    
                        
                src:SHP_END,
                src:SHP_PERSON,
                src:SHP_SHIFT,
                src:SHP_START,
            src:SHP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:SHP_END,
                src:SHP_PERSON,
                src:SHP_SHIFT,
                src:SHP_START  ORDER BY 
            src:SHP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5SHFPERS as strm))
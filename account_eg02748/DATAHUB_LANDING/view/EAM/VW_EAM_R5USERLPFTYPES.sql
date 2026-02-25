CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5USERLPFTYPES AS SELECT
                        src:LPT_LASTSAVED::datetime AS LPT_LASTSAVED, 
                        src:LPT_LINEARPREFERENCE::varchar AS LPT_LINEARPREFERENCE, 
                        src:LPT_RTYPE::varchar AS LPT_RTYPE, 
                        src:LPT_SEQUENCE::numeric(38, 10) AS LPT_SEQUENCE, 
                        src:LPT_TYPE::varchar AS LPT_TYPE, 
                        src:LPT_UPDATECOUNT::numeric(38, 10) AS LPT_UPDATECOUNT, 
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
    
                        
                src:LPT_LINEARPREFERENCE,
                src:LPT_TYPE,
            src:LPT_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:LPT_LINEARPREFERENCE,
                src:LPT_TYPE  ORDER BY 
            src:LPT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5USERLPFTYPES as strm))
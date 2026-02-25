CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5GRIDPARAM AS SELECT
                        src:GDP_DATATYPE::varchar AS GDP_DATATYPE, 
                        src:GDP_GRIDID::numeric(38, 10) AS GDP_GRIDID, 
                        src:GDP_LASTSAVED::datetime AS GDP_LASTSAVED, 
                        src:GDP_PARAM::varchar AS GDP_PARAM, 
                        src:GDP_TAGNAME::varchar AS GDP_TAGNAME, 
                        src:GDP_UPDATECOUNT::numeric(38, 10) AS GDP_UPDATECOUNT, 
                        src:GDP_UPDATED::datetime AS GDP_UPDATED, 
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
    
                        
                src:GDP_GRIDID,
                src:GDP_PARAM,
            src:GDP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:GDP_GRIDID,
                src:GDP_PARAM  ORDER BY 
            src:GDP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5GRIDPARAM as strm))
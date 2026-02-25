CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5FUNNOPERM AS SELECT
                        src:FPN_FUNCTION::varchar AS FPN_FUNCTION, 
                        src:FPN_LASTSAVED::datetime AS FPN_LASTSAVED, 
                        src:FPN_NODELETE::varchar AS FPN_NODELETE, 
                        src:FPN_NOINSERT::varchar AS FPN_NOINSERT, 
                        src:FPN_NOSELECT::varchar AS FPN_NOSELECT, 
                        src:FPN_NOUPDATE::varchar AS FPN_NOUPDATE, 
                        src:FPN_UPDATECOUNT::numeric(38, 10) AS FPN_UPDATECOUNT, 
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
    
                        
                src:FPN_FUNCTION,
            src:FPN_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:FPN_FUNCTION  ORDER BY 
            src:FPN_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5FUNNOPERM as strm))
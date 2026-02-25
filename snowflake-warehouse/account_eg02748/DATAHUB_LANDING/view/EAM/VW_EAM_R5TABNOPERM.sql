CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5TABNOPERM AS SELECT
                        src:TPN_FUNCTION::varchar AS TPN_FUNCTION, 
                        src:TPN_LASTSAVED::datetime AS TPN_LASTSAVED, 
                        src:TPN_NODELETE::varchar AS TPN_NODELETE, 
                        src:TPN_NOINSERT::varchar AS TPN_NOINSERT, 
                        src:TPN_NOSELECT::varchar AS TPN_NOSELECT, 
                        src:TPN_NOUPDATE::varchar AS TPN_NOUPDATE, 
                        src:TPN_TAB::varchar AS TPN_TAB, 
                        src:TPN_UPDATECOUNT::numeric(38, 10) AS TPN_UPDATECOUNT, 
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
    
                        
                src:TPN_FUNCTION,
                src:TPN_TAB,
            src:TPN_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:TPN_FUNCTION,
                src:TPN_TAB  ORDER BY 
            src:TPN_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5TABNOPERM as strm))
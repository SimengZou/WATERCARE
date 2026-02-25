CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5REVCNTRLSETUP AS SELECT
                        src:RCS_ELEMENTID::varchar AS RCS_ELEMENTID, 
                        src:RCS_LASTSAVED::datetime AS RCS_LASTSAVED, 
                        src:RCS_PAGENAME::varchar AS RCS_PAGENAME, 
                        src:RCS_PROTECTED::varchar AS RCS_PROTECTED, 
                        src:RCS_UPDATECOUNT::numeric(38, 10) AS RCS_UPDATECOUNT, 
                        src:RCS_XPATH::varchar AS RCS_XPATH, 
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
    
                        
                src:RCS_ELEMENTID,
                src:RCS_PAGENAME,
            src:RCS_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:RCS_ELEMENTID,
                src:RCS_PAGENAME  ORDER BY 
            src:RCS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5REVCNTRLSETUP as strm))
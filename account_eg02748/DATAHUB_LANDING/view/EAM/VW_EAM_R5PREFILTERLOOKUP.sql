CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5PREFILTERLOOKUP AS SELECT
                        src:PFE_ELEMENTID::varchar AS PFE_ELEMENTID, 
                        src:PFE_FILTERSTRXML::varchar AS PFE_FILTERSTRXML, 
                        src:PFE_LASTSAVED::datetime AS PFE_LASTSAVED, 
                        src:PFE_PAGENAME::varchar AS PFE_PAGENAME, 
                        src:PFE_UPDATECOUNT::numeric(38, 10) AS PFE_UPDATECOUNT, 
                        src:PFE_USERFILTER::varchar AS PFE_USERFILTER, 
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
    
                        
                src:PFE_ELEMENTID,
                src:PFE_PAGENAME,
            src:PFE_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PFE_ELEMENTID,
                src:PFE_PAGENAME  ORDER BY 
            src:PFE_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5PREFILTERLOOKUP as strm))
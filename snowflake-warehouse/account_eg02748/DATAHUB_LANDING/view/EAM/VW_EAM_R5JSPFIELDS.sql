CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5JSPFIELDS AS SELECT
                        src:JFD_FIELDS::varchar AS JFD_FIELDS, 
                        src:JFD_JSINCLUDES::varchar AS JFD_JSINCLUDES, 
                        src:JFD_JSPFILE::varchar AS JFD_JSPFILE, 
                        src:JFD_LASTSAVED::datetime AS JFD_LASTSAVED, 
                        src:JFD_OTHERFIELDS::varchar AS JFD_OTHERFIELDS, 
                        src:JFD_UPDATECOUNT::numeric(38, 10) AS JFD_UPDATECOUNT, 
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
    
                        
                src:JFD_JSPFILE,
            src:JFD_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:JFD_JSPFILE  ORDER BY 
            src:JFD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5JSPFIELDS as strm))
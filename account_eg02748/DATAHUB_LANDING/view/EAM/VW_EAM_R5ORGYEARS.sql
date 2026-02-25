CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ORGYEARS AS SELECT
                        src:OYE_END::datetime AS OYE_END, 
                        src:OYE_LASTSAVED::datetime AS OYE_LASTSAVED, 
                        src:OYE_ORG::varchar AS OYE_ORG, 
                        src:OYE_PK::numeric(38, 10) AS OYE_PK, 
                        src:OYE_START::datetime AS OYE_START, 
                        src:OYE_UPDATECOUNT::numeric(38, 10) AS OYE_UPDATECOUNT, 
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
    
                        
                src:OYE_PK,
            src:OYE_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:OYE_PK  ORDER BY 
            src:OYE_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ORGYEARS as strm))
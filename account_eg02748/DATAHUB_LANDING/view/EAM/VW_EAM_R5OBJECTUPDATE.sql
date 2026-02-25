CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5OBJECTUPDATE AS SELECT
                        src:OUP_DATE::datetime AS OUP_DATE, 
                        src:OUP_LASTSAVED::datetime AS OUP_LASTSAVED, 
                        src:OUP_NEWCODE::varchar AS OUP_NEWCODE, 
                        src:OUP_OLDCODE::varchar AS OUP_OLDCODE, 
                        src:OUP_ORG::varchar AS OUP_ORG, 
                        src:OUP_PK::numeric(38, 10) AS OUP_PK, 
                        src:OUP_USER::varchar AS OUP_USER, 
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
    
                        
                src:OUP_PK,
            src:OUP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:OUP_PK  ORDER BY 
            src:OUP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5OBJECTUPDATE as strm))
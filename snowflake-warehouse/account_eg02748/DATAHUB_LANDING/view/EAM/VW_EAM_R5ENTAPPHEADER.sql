CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ENTAPPHEADER AS SELECT
                        src:EAH_APPDATE::datetime AS EAH_APPDATE, 
                        src:EAH_APPLIST::varchar AS EAH_APPLIST, 
                        src:EAH_CODE::varchar AS EAH_CODE, 
                        src:EAH_DATE::datetime AS EAH_DATE, 
                        src:EAH_ENTITY::varchar AS EAH_ENTITY, 
                        src:EAH_LASTSAVED::datetime AS EAH_LASTSAVED, 
                        src:EAH_PK::numeric(38, 10) AS EAH_PK, 
                        src:EAH_REASON::varchar AS EAH_REASON, 
                        src:EAH_RENTITY::varchar AS EAH_RENTITY, 
                        src:EAH_REVISION::numeric(38, 10) AS EAH_REVISION, 
                        src:EAH_UPDATECOUNT::numeric(38, 10) AS EAH_UPDATECOUNT, 
                        src:EAH_USER::varchar AS EAH_USER, 
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
    
                        
                src:EAH_PK,
            src:EAH_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:EAH_PK  ORDER BY 
            src:EAH_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ENTAPPHEADER as strm))
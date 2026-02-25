CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5AUTH AS SELECT
                        src:AUT_CREATED::datetime AS AUT_CREATED, 
                        src:AUT_ENTITY::varchar AS AUT_ENTITY, 
                        src:AUT_GROUP::varchar AS AUT_GROUP, 
                        src:AUT_LASTSAVED::datetime AS AUT_LASTSAVED, 
                        src:AUT_RENTITY::varchar AS AUT_RENTITY, 
                        src:AUT_STATNEW::varchar AS AUT_STATNEW, 
                        src:AUT_STATUS::varchar AS AUT_STATUS, 
                        src:AUT_TYPE::varchar AS AUT_TYPE, 
                        src:AUT_UPDATECOUNT::numeric(38, 10) AS AUT_UPDATECOUNT, 
                        src:AUT_UPDATED::datetime AS AUT_UPDATED, 
                        src:AUT_USER::varchar AS AUT_USER, 
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
    
                        
                src:AUT_ENTITY,
                src:AUT_GROUP,
                src:AUT_STATNEW,
                src:AUT_STATUS,
                src:AUT_USER,
            src:AUT_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:AUT_ENTITY,
                src:AUT_GROUP,
                src:AUT_STATNEW,
                src:AUT_STATUS,
                src:AUT_USER  ORDER BY 
            src:AUT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5AUTH as strm))
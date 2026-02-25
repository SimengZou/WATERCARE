CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ADDETAILS AS SELECT
                        src:ADD_CODE::varchar AS ADD_CODE, 
                        src:ADD_CREATED::datetime AS ADD_CREATED, 
                        src:ADD_ENTITY::varchar AS ADD_ENTITY, 
                        src:ADD_LANG::varchar AS ADD_LANG, 
                        src:ADD_LASTSAVED::datetime AS ADD_LASTSAVED, 
                        src:ADD_LINE::numeric(38, 10) AS ADD_LINE, 
                        src:ADD_PRINT::varchar AS ADD_PRINT, 
                        src:ADD_RENTITY::varchar AS ADD_RENTITY, 
                        src:ADD_RTYPE::varchar AS ADD_RTYPE, 
                        src:ADD_TEXT::varchar AS ADD_TEXT, 
                        src:ADD_TYPE::varchar AS ADD_TYPE, 
                        src:ADD_UPDATECOUNT::numeric(38, 10) AS ADD_UPDATECOUNT, 
                        src:ADD_UPDATED::datetime AS ADD_UPDATED, 
                        src:ADD_UPDUSER::varchar AS ADD_UPDUSER, 
                        src:ADD_USER::varchar AS ADD_USER, 
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
    
                        
                src:ADD_CODE,
                src:ADD_LANG,
                src:ADD_LINE,
                src:ADD_RENTITY,
                src:ADD_TYPE,
            src:ADD_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ADD_CODE,
                src:ADD_LANG,
                src:ADD_LINE,
                src:ADD_RENTITY,
                src:ADD_TYPE  ORDER BY 
            src:ADD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ADDETAILS as strm))
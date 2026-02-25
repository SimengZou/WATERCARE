CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5DEVICES AS SELECT
                        src:DEV_CATEGORY::varchar AS DEV_CATEGORY, 
                        src:DEV_CATFLAG::varchar AS DEV_CATFLAG, 
                        src:DEV_CODE::varchar AS DEV_CODE, 
                        src:DEV_DESC::varchar AS DEV_DESC, 
                        src:DEV_DESTINATION::varchar AS DEV_DESTINATION, 
                        src:DEV_DRIVER::varchar AS DEV_DRIVER, 
                        src:DEV_LASTLOGINDATE::datetime AS DEV_LASTLOGINDATE, 
                        src:DEV_LASTSAVED::datetime AS DEV_LASTSAVED, 
                        src:DEV_MODE::varchar AS DEV_MODE, 
                        src:DEV_NOTUSED::varchar AS DEV_NOTUSED, 
                        src:DEV_ORG::varchar AS DEV_ORG, 
                        src:DEV_ORIENTATION::varchar AS DEV_ORIENTATION, 
                        src:DEV_RTYPE::varchar AS DEV_RTYPE, 
                        src:DEV_SPECIAL::varchar AS DEV_SPECIAL, 
                        src:DEV_TYPE::varchar AS DEV_TYPE, 
                        src:DEV_UPDATECOUNT::numeric(38, 10) AS DEV_UPDATECOUNT, 
                        src:DEV_UPDATED::datetime AS DEV_UPDATED, 
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
    
                        
                src:DEV_CODE,
            src:DEV_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:DEV_CODE  ORDER BY 
            src:DEV_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5DEVICES as strm))
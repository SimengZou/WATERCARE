CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5CURRENCIES AS SELECT
                        src:CUR_CLASS::varchar AS CUR_CLASS, 
                        src:CUR_CLASS_ORG::varchar AS CUR_CLASS_ORG, 
                        src:CUR_CODE::varchar AS CUR_CODE, 
                        src:CUR_CREATED::datetime AS CUR_CREATED, 
                        src:CUR_DESC::varchar AS CUR_DESC, 
                        src:CUR_DUAL::numeric(38, 10) AS CUR_DUAL, 
                        src:CUR_INTERFACE::datetime AS CUR_INTERFACE, 
                        src:CUR_LASTSAVED::datetime AS CUR_LASTSAVED, 
                        src:CUR_NOTUSED::varchar AS CUR_NOTUSED, 
                        src:CUR_SOURCECODE::varchar AS CUR_SOURCECODE, 
                        src:CUR_SOURCESYSTEM::varchar AS CUR_SOURCESYSTEM, 
                        src:CUR_UPDATECOUNT::numeric(38, 10) AS CUR_UPDATECOUNT, 
                        src:CUR_UPDATED::datetime AS CUR_UPDATED, 
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
    
                        
                src:CUR_CODE,
            src:CUR_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CUR_CODE  ORDER BY 
            src:CUR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5CURRENCIES as strm))
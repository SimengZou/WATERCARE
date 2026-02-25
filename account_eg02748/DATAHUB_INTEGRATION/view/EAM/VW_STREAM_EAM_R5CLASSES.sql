CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CLASSES AS SELECT
                        src:CLS_CODE::varchar AS CLS_CODE, 
                        src:CLS_CREATED::datetime AS CLS_CREATED, 
                        src:CLS_DESC::varchar AS CLS_DESC, 
                        src:CLS_ENTITY::varchar AS CLS_ENTITY, 
                        src:CLS_ICON::varchar AS CLS_ICON, 
                        src:CLS_ICONPATH::varchar AS CLS_ICONPATH, 
                        src:CLS_LASTSAVED::datetime AS CLS_LASTSAVED, 
                        src:CLS_LEVEL::numeric(38, 10) AS CLS_LEVEL, 
                        src:CLS_MOBILE_NOTEBOOK_DEFINITION::varchar AS CLS_MOBILE_NOTEBOOK_DEFINITION, 
                        src:CLS_NOTUSED::varchar AS CLS_NOTUSED, 
                        src:CLS_ORG::varchar AS CLS_ORG, 
                        src:CLS_PROPERTY_DEFINITION::varchar AS CLS_PROPERTY_DEFINITION, 
                        src:CLS_RENTITY::varchar AS CLS_RENTITY, 
                        src:CLS_RENTITYCODE::varchar AS CLS_RENTITYCODE, 
                        src:CLS_UPDATECOUNT::numeric(38, 10) AS CLS_UPDATECOUNT, 
                        src:CLS_UPDATED::datetime AS CLS_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:CLS_LASTSAVED::datetime as ETL_LASTSAVED,
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CLS_CODE,
                src:CLS_ENTITY,
                src:CLS_ORG  ORDER BY 
            src:CLS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CLASSES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CLS_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CLS_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CLS_LEVEL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CLS_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CLS_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CLS_LASTSAVED), '1900-01-01')) is not null
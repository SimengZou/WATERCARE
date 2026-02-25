CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CURRENCIES AS SELECT
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
                        src:CUR_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:CUR_CODE  ORDER BY 
            src:CUR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CURRENCIES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CUR_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CUR_DUAL), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CUR_INTERFACE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CUR_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CUR_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CUR_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CUR_LASTSAVED), '1900-01-01')) is not null
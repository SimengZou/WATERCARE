CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CONFIGSETTINGS AS SELECT
                        src:CFS_CODE::varchar AS CFS_CODE, 
                        src:CFS_COMPTYPE::varchar AS CFS_COMPTYPE, 
                        src:CFS_CONFIG::numeric(38, 10) AS CFS_CONFIG, 
                        src:CFS_CREATED::datetime AS CFS_CREATED, 
                        src:CFS_DESK::varchar AS CFS_DESK, 
                        src:CFS_GROUP::varchar AS CFS_GROUP, 
                        src:CFS_LASTSAVED::datetime AS CFS_LASTSAVED, 
                        src:CFS_UPDATECOUNT::numeric(38, 10) AS CFS_UPDATECOUNT, 
                        src:CFS_UPDATED::datetime AS CFS_UPDATED, 
                        src:CFS_USER::varchar AS CFS_USER, 
                        src:CFS_XMLCONFIG::varchar AS CFS_XMLCONFIG, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:CFS_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:CFS_CODE,
                src:CFS_COMPTYPE,
                src:CFS_CONFIG,
                src:CFS_DESK,
                src:CFS_GROUP,
                src:CFS_USER  ORDER BY 
            src:CFS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CONFIGSETTINGS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CFS_CONFIG), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CFS_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CFS_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CFS_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CFS_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CFS_LASTSAVED), '1900-01-01')) is not null
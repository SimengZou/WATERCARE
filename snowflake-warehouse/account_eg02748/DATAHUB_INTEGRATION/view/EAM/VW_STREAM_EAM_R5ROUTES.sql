CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ROUTES AS SELECT
                        src:ROU_CATEGORY::varchar AS ROU_CATEGORY, 
                        src:ROU_CLASS::varchar AS ROU_CLASS, 
                        src:ROU_CLASS_ORG::varchar AS ROU_CLASS_ORG, 
                        src:ROU_CODE::varchar AS ROU_CODE, 
                        src:ROU_DESC::varchar AS ROU_DESC, 
                        src:ROU_LASTSAVED::datetime AS ROU_LASTSAVED, 
                        src:ROU_LINKGISWO::varchar AS ROU_LINKGISWO, 
                        src:ROU_ORG::varchar AS ROU_ORG, 
                        src:ROU_REVISION::numeric(38, 10) AS ROU_REVISION, 
                        src:ROU_REVRSTATUS::varchar AS ROU_REVRSTATUS, 
                        src:ROU_REVSTATUS::varchar AS ROU_REVSTATUS, 
                        src:ROU_TEMPLATE::varchar AS ROU_TEMPLATE, 
                        src:ROU_UPDATECOUNT::numeric(38, 10) AS ROU_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ROU_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ROU_CODE,
                src:ROU_REVISION  ORDER BY 
            src:ROU_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ROUTES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ROU_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROU_REVISION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROU_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ROU_LASTSAVED), '1900-01-01')) is not null
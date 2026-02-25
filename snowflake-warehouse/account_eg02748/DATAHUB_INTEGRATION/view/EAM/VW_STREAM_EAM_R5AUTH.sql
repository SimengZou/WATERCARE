CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5AUTH AS SELECT
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
                        src:AUT_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:AUT_ENTITY,
                src:AUT_GROUP,
                src:AUT_STATNEW,
                src:AUT_STATUS,
                src:AUT_USER  ORDER BY 
            src:AUT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5AUTH as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AUT_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AUT_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AUT_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AUT_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AUT_LASTSAVED), '1900-01-01')) is not null
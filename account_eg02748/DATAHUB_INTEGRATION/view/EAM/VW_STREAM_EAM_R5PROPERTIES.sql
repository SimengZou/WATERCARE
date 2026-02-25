CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PROPERTIES AS SELECT
                        src:PRO_CODE::varchar AS PRO_CODE, 
                        src:PRO_CREATED::datetime AS PRO_CREATED, 
                        src:PRO_INCLUDEINGRIDS::varchar AS PRO_INCLUDEINGRIDS, 
                        src:PRO_LASTSAVED::datetime AS PRO_LASTSAVED, 
                        src:PRO_MAX::varchar AS PRO_MAX, 
                        src:PRO_MIN::varchar AS PRO_MIN, 
                        src:PRO_RENTITY::varchar AS PRO_RENTITY, 
                        src:PRO_TEXT::varchar AS PRO_TEXT, 
                        src:PRO_TYPE::varchar AS PRO_TYPE, 
                        src:PRO_UPDATECOUNT::numeric(38, 10) AS PRO_UPDATECOUNT, 
                        src:PRO_UPDATED::datetime AS PRO_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PRO_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PRO_CODE  ORDER BY 
            src:PRO_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PROPERTIES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PRO_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PRO_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRO_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PRO_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PRO_LASTSAVED), '1900-01-01')) is not null
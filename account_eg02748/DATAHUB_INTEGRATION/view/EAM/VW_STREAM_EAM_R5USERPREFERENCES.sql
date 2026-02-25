CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USERPREFERENCES AS SELECT
                        src:USP_CODE::varchar AS USP_CODE, 
                        src:USP_LASTSAVED::datetime AS USP_LASTSAVED, 
                        src:USP_TYPE::varchar AS USP_TYPE, 
                        src:USP_UPDATECOUNT::numeric(38, 10) AS USP_UPDATECOUNT, 
                        src:USP_USER::varchar AS USP_USER, 
                        src:USP_VALUE::varchar AS USP_VALUE, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:USP_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:USP_CODE,
                src:USP_TYPE,
                src:USP_USER  ORDER BY 
            src:USP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5USERPREFERENCES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:USP_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USP_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:USP_LASTSAVED), '1900-01-01')) is not null
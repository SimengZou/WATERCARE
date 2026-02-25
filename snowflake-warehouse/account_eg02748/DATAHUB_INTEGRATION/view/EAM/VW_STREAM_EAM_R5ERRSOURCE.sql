CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ERRSOURCE AS SELECT
                        src:ERS_CODE::varchar AS ERS_CODE, 
                        src:ERS_DESC::varchar AS ERS_DESC, 
                        src:ERS_LASTSAVED::datetime AS ERS_LASTSAVED, 
                        src:ERS_NAME::varchar AS ERS_NAME, 
                        src:ERS_NUMBER::numeric(38, 10) AS ERS_NUMBER, 
                        src:ERS_SOURCE::varchar AS ERS_SOURCE, 
                        src:ERS_TYPE::varchar AS ERS_TYPE, 
                        src:ERS_UPDATECOUNT::numeric(38, 10) AS ERS_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ERS_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ERS_NUMBER,
                src:ERS_SOURCE,
                src:ERS_TYPE  ORDER BY 
            src:ERS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ERRSOURCE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ERS_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ERS_NUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ERS_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ERS_LASTSAVED), '1900-01-01')) is not null
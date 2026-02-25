CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PASSWORDS AS SELECT
                        src:PWD_LASTSAVED::datetime AS PWD_LASTSAVED, 
                        src:PWD_LASTUSED::datetime AS PWD_LASTUSED, 
                        src:PWD_PASSWORD::varchar AS PWD_PASSWORD, 
                        src:PWD_UPDATECOUNT::numeric(38, 10) AS PWD_UPDATECOUNT, 
                        src:PWD_USER::varchar AS PWD_USER, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PWD_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PWD_LASTUSED,
                src:PWD_PASSWORD,
                src:PWD_USER  ORDER BY 
            src:PWD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PASSWORDS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PWD_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PWD_LASTUSED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PWD_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PWD_LASTSAVED), '1900-01-01')) is not null
CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ALLTEXTS AS SELECT
                        src:ATX_CODE::varchar AS ATX_CODE, 
                        src:ATX_LANG::varchar AS ATX_LANG, 
                        src:ATX_LASTMODIFIED::datetime AS ATX_LASTMODIFIED, 
                        src:ATX_LASTSAVED::datetime AS ATX_LASTSAVED, 
                        src:ATX_TEXT::varchar AS ATX_TEXT, 
                        src:ATX_TEXTTYPE::varchar AS ATX_TEXTTYPE, 
                        src:ATX_UPDATECOUNT::numeric(38, 10) AS ATX_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ATX_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ATX_CODE,
                src:ATX_LANG,
                src:ATX_TEXTTYPE  ORDER BY 
            src:ATX_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALLTEXTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ATX_LASTMODIFIED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ATX_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ATX_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ATX_LASTSAVED), '1900-01-01')) is not null
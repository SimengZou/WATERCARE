CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5AUDATTRIBS AS SELECT
                        src:AAT_CODE::numeric(38, 10) AS AAT_CODE, 
                        src:AAT_COLUMN::varchar AS AAT_COLUMN, 
                        src:AAT_COMMENT::varchar AS AAT_COMMENT, 
                        src:AAT_DELETE::varchar AS AAT_DELETE, 
                        src:AAT_ENTEREDBY::varchar AS AAT_ENTEREDBY, 
                        src:AAT_INSERT::varchar AS AAT_INSERT, 
                        src:AAT_LASTSAVED::datetime AS AAT_LASTSAVED, 
                        src:AAT_TABLE::varchar AS AAT_TABLE, 
                        src:AAT_TEXT::varchar AS AAT_TEXT, 
                        src:AAT_UPDATE::varchar AS AAT_UPDATE, 
                        src:AAT_UPDATECOUNT::numeric(38, 10) AS AAT_UPDATECOUNT, 
                        src:AAT_UPDATED::datetime AS AAT_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:AAT_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:AAT_CODE  ORDER BY 
            src:AAT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5AUDATTRIBS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AAT_CODE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AAT_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AAT_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AAT_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AAT_LASTSAVED), '1900-01-01')) is not null
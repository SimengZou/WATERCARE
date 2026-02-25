CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WSPROMPTS AS SELECT
                        src:WST_CODE::varchar AS WST_CODE, 
                        src:WST_DESC::varchar AS WST_DESC, 
                        src:WST_FUNCTION::varchar AS WST_FUNCTION, 
                        src:WST_LASTSAVED::datetime AS WST_LASTSAVED, 
                        src:WST_NOTUSED::varchar AS WST_NOTUSED, 
                        src:WST_SYSTEM::varchar AS WST_SYSTEM, 
                        src:WST_TAB::varchar AS WST_TAB, 
                        src:WST_UPDATECOUNT::numeric(38, 10) AS WST_UPDATECOUNT, 
                        src:WST_UPDATED::datetime AS WST_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:WST_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:WST_CODE  ORDER BY 
            src:WST_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5WSPROMPTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WST_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WST_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WST_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WST_LASTSAVED), '1900-01-01')) is not null
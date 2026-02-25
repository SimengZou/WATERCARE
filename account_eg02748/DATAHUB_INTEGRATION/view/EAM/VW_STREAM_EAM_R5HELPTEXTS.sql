CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5HELPTEXTS AS SELECT
                        src:HEL_CHANGED::varchar AS HEL_CHANGED, 
                        src:HEL_DRILLDOWN::varchar AS HEL_DRILLDOWN, 
                        src:HEL_FUNCTION::varchar AS HEL_FUNCTION, 
                        src:HEL_ITEM::varchar AS HEL_ITEM, 
                        src:HEL_LANG::varchar AS HEL_LANG, 
                        src:HEL_LASTSAVED::datetime AS HEL_LASTSAVED, 
                        src:HEL_POOL::varchar AS HEL_POOL, 
                        src:HEL_TEXT::varchar AS HEL_TEXT, 
                        src:HEL_UPDATECOUNT::numeric(38, 10) AS HEL_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:HEL_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:HEL_FUNCTION,
                src:HEL_ITEM,
                src:HEL_LANG  ORDER BY 
            src:HEL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5HELPTEXTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:HEL_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HEL_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:HEL_LASTSAVED), '1900-01-01')) is not null
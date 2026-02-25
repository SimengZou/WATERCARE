CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FILTERTABCOLUMNS AS SELECT
                        src:FTC_CLOB::varchar AS FTC_CLOB, 
                        src:FTC_COLUMN::varchar AS FTC_COLUMN, 
                        src:FTC_DATATYPE::varchar AS FTC_DATATYPE, 
                        src:FTC_LASTSAVED::datetime AS FTC_LASTSAVED, 
                        src:FTC_TABLE::varchar AS FTC_TABLE, 
                        src:FTC_UPDATECOUNT::numeric(38, 10) AS FTC_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:FTC_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:FTC_COLUMN,
                src:FTC_TABLE  ORDER BY 
            src:FTC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5FILTERTABCOLUMNS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:FTC_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FTC_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:FTC_LASTSAVED), '1900-01-01')) is not null
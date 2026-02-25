CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPORTSUBTOTAL AS SELECT
                        src:RST_BOTNUMBER::numeric(38, 10) AS RST_BOTNUMBER, 
                        src:RST_DATATYPE::varchar AS RST_DATATYPE, 
                        src:RST_FIELD::varchar AS RST_FIELD, 
                        src:RST_FUNCTION::varchar AS RST_FUNCTION, 
                        src:RST_GROUPLINE::numeric(38, 10) AS RST_GROUPLINE, 
                        src:RST_LASTSAVED::datetime AS RST_LASTSAVED, 
                        src:RST_LINE::numeric(38, 10) AS RST_LINE, 
                        src:RST_UPDATECOUNT::numeric(38, 10) AS RST_UPDATECOUNT, 
                        src:RST_UPDATED::datetime AS RST_UPDATED, 
                        src:RST_WIDTH::numeric(38, 10) AS RST_WIDTH, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:RST_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:RST_FUNCTION,
                src:RST_GROUPLINE,
                src:RST_LINE  ORDER BY 
            src:RST_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5REPORTSUBTOTAL as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RST_BOTNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RST_GROUPLINE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RST_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RST_LINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RST_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RST_UPDATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RST_WIDTH), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RST_LASTSAVED), '1900-01-01')) is not null
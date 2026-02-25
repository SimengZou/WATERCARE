CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPORTFIELDS AS SELECT
                        src:RFL_BOTNUMBER::numeric(38, 10) AS RFL_BOTNUMBER, 
                        src:RFL_DATATYPE::varchar AS RFL_DATATYPE, 
                        src:RFL_FIELD::varchar AS RFL_FIELD, 
                        src:RFL_FUNCTION::varchar AS RFL_FUNCTION, 
                        src:RFL_LASTSAVED::datetime AS RFL_LASTSAVED, 
                        src:RFL_LINE::numeric(38, 10) AS RFL_LINE, 
                        src:RFL_SHOWFIELD::varchar AS RFL_SHOWFIELD, 
                        src:RFL_UPDATECOUNT::numeric(38, 10) AS RFL_UPDATECOUNT, 
                        src:RFL_UPDATED::datetime AS RFL_UPDATED, 
                        src:RFL_WIDTH::numeric(38, 10) AS RFL_WIDTH, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:RFL_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:RFL_FUNCTION,
                src:RFL_LINE  ORDER BY 
            src:RFL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5REPORTFIELDS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RFL_BOTNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RFL_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RFL_LINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RFL_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RFL_UPDATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RFL_WIDTH), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RFL_LASTSAVED), '1900-01-01')) is not null
CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PMPSESSIONLINES AS SELECT
                        src:PPL_LASTSAVED::datetime AS PPL_LASTSAVED, 
                        src:PPL_LINE::numeric(38, 10) AS PPL_LINE, 
                        src:PPL_NESTINGREF::varchar AS PPL_NESTINGREF, 
                        src:PPL_OBJECT::varchar AS PPL_OBJECT, 
                        src:PPL_OBJORG::varchar AS PPL_OBJORG, 
                        src:PPL_PPM::varchar AS PPL_PPM, 
                        src:PPL_PPOPK::numeric(38, 10) AS PPL_PPOPK, 
                        src:PPL_SESSIONID::numeric(38, 10) AS PPL_SESSIONID, 
                        src:PPL_UPDATECOUNT::numeric(38, 10) AS PPL_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PPL_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PPL_LINE  ORDER BY 
            src:PPL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PMPSESSIONLINES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPL_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPL_LINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPL_PPOPK), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPL_SESSIONID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPL_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPL_LASTSAVED), '1900-01-01')) is not null
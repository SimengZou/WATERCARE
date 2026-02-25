CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPOPTIONS AS SELECT
                        src:ROP_FUNCTION::varchar AS ROP_FUNCTION, 
                        src:ROP_LASTSAVED::datetime AS ROP_LASTSAVED, 
                        src:ROP_MEKEY::varchar AS ROP_MEKEY, 
                        src:ROP_PARAMETER::varchar AS ROP_PARAMETER, 
                        src:ROP_SEQNO::numeric(38, 10) AS ROP_SEQNO, 
                        src:ROP_UPDATECOUNT::numeric(38, 10) AS ROP_UPDATECOUNT, 
                        src:ROP_UPDATED::datetime AS ROP_UPDATED, 
                        src:ROP_VALUE::varchar AS ROP_VALUE, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ROP_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ROP_FUNCTION,
                src:ROP_PARAMETER,
                src:ROP_SEQNO  ORDER BY 
            src:ROP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5REPOPTIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ROP_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROP_SEQNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROP_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ROP_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ROP_LASTSAVED), '1900-01-01')) is not null
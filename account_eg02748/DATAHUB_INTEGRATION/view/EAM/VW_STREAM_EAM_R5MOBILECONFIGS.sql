CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MOBILECONFIGS AS SELECT
                        src:MCF_CODE::varchar AS MCF_CODE, 
                        src:MCF_COMPTYPE::varchar AS MCF_COMPTYPE, 
                        src:MCF_CONFIG::numeric(38, 10) AS MCF_CONFIG, 
                        src:MCF_CREATED::datetime AS MCF_CREATED, 
                        src:MCF_DESK::varchar AS MCF_DESK, 
                        src:MCF_GROUP::varchar AS MCF_GROUP, 
                        src:MCF_LASTSAVED::datetime AS MCF_LASTSAVED, 
                        src:MCF_RCODE::varchar AS MCF_RCODE, 
                        src:MCF_UPDATECOUNT::numeric(38, 10) AS MCF_UPDATECOUNT, 
                        src:MCF_UPDATED::datetime AS MCF_UPDATED, 
                        src:MCF_USER::varchar AS MCF_USER, 
                        src:MCF_XMLCONFIG::varchar AS MCF_XMLCONFIG, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:MCF_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:MCF_CODE,
                src:MCF_CONFIG,
                src:MCF_DESK,
                src:MCF_RCODE  ORDER BY 
            src:MCF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MOBILECONFIGS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MCF_CONFIG), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MCF_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MCF_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MCF_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MCF_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MCF_LASTSAVED), '1900-01-01')) is not null
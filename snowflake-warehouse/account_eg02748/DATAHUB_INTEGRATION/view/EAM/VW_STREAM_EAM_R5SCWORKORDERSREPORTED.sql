CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5SCWORKORDERSREPORTED AS SELECT
                        src:CWR_DATE::datetime AS CWR_DATE, 
                        src:CWR_LASTSAVED::datetime AS CWR_LASTSAVED, 
                        src:CWR_MRC::varchar AS CWR_MRC, 
                        src:CWR_ORG::varchar AS CWR_ORG, 
                        src:CWR_WOSREPORTED::numeric(38, 10) AS CWR_WOSREPORTED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:CWR_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:CWR_DATE,
                src:CWR_MRC,
                src:CWR_ORG  ORDER BY 
            src:CWR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5SCWORKORDERSREPORTED as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CWR_DATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CWR_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CWR_WOSREPORTED), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CWR_LASTSAVED), '1900-01-01')) is not null
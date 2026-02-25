CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CHARGEDEFSEQUENCE AS SELECT
                        src:CDS_ACTUALSUBCAT::varchar AS CDS_ACTUALSUBCAT, 
                        src:CDS_CATEGORY::varchar AS CDS_CATEGORY, 
                        src:CDS_LASTSAVED::datetime AS CDS_LASTSAVED, 
                        src:CDS_LEVEL::varchar AS CDS_LEVEL, 
                        src:CDS_SEQUENCE::numeric(38, 10) AS CDS_SEQUENCE, 
                        src:CDS_SUBCATEGORY::varchar AS CDS_SUBCATEGORY, 
                        src:CDS_UPDATECOUNT::numeric(38, 10) AS CDS_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:CDS_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:CDS_CATEGORY,
                src:CDS_LEVEL,
                src:CDS_SEQUENCE,
                src:CDS_SUBCATEGORY  ORDER BY 
            src:CDS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CHARGEDEFSEQUENCE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CDS_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CDS_SEQUENCE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CDS_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CDS_LASTSAVED), '1900-01-01')) is not null
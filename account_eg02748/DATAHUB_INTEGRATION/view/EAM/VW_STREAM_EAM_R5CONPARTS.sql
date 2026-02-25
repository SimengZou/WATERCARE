CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CONPARTS AS SELECT
                        src:CPA_CONTRACT::varchar AS CPA_CONTRACT, 
                        src:CPA_LASTSAVED::datetime AS CPA_LASTSAVED, 
                        src:CPA_LEADTIME::numeric(38, 10) AS CPA_LEADTIME, 
                        src:CPA_MULTIPLY::numeric(38, 10) AS CPA_MULTIPLY, 
                        src:CPA_PART::varchar AS CPA_PART, 
                        src:CPA_PART_ORG::varchar AS CPA_PART_ORG, 
                        src:CPA_PRICE::numeric(38, 10) AS CPA_PRICE, 
                        src:CPA_PURUOM::varchar AS CPA_PURUOM, 
                        src:CPA_REF::varchar AS CPA_REF, 
                        src:CPA_SUPPLIER::varchar AS CPA_SUPPLIER, 
                        src:CPA_SUPPLIER_ORG::varchar AS CPA_SUPPLIER_ORG, 
                        src:CPA_UPDATECOUNT::numeric(38, 10) AS CPA_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:CPA_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:CPA_CONTRACT,
                src:CPA_PART,
                src:CPA_PART_ORG  ORDER BY 
            src:CPA_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CONPARTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CPA_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CPA_LEADTIME), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CPA_MULTIPLY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CPA_PRICE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CPA_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CPA_LASTSAVED), '1900-01-01')) is not null
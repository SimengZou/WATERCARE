CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TASKPRICES AS SELECT
                        src:TPR_LASTSAVED::datetime AS TPR_LASTSAVED, 
                        src:TPR_ORG::varchar AS TPR_ORG, 
                        src:TPR_PRICE::numeric(38, 10) AS TPR_PRICE, 
                        src:TPR_REVISION::numeric(38, 10) AS TPR_REVISION, 
                        src:TPR_TASK::varchar AS TPR_TASK, 
                        src:TPR_UPDATECOUNT::numeric(38, 10) AS TPR_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:TPR_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:TPR_ORG,
                src:TPR_REVISION,
                src:TPR_TASK  ORDER BY 
            src:TPR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5TASKPRICES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TPR_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TPR_PRICE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TPR_REVISION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TPR_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TPR_LASTSAVED), '1900-01-01')) is not null
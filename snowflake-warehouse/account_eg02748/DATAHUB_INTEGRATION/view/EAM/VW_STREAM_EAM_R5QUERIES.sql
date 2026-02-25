CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5QUERIES AS SELECT
                        src:QUE_ASSET::varchar AS QUE_ASSET, 
                        src:QUE_CHART::varchar AS QUE_CHART, 
                        src:QUE_CODE::varchar AS QUE_CODE, 
                        src:QUE_DESC::varchar AS QUE_DESC, 
                        src:QUE_EQUIPMENTRANKING::varchar AS QUE_EQUIPMENTRANKING, 
                        src:QUE_INBOX::varchar AS QUE_INBOX, 
                        src:QUE_KPI::varchar AS QUE_KPI, 
                        src:QUE_LASTSAVED::datetime AS QUE_LASTSAVED, 
                        src:QUE_LOOKUP::varchar AS QUE_LOOKUP, 
                        src:QUE_NORMAL::varchar AS QUE_NORMAL, 
                        src:QUE_NOTE::varchar AS QUE_NOTE, 
                        src:QUE_TEXT::varchar AS QUE_TEXT, 
                        src:QUE_UPDATECOUNT::numeric(38, 10) AS QUE_UPDATECOUNT, 
                        src:QUE_UPDATED::datetime AS QUE_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:QUE_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:QUE_CODE  ORDER BY 
            src:QUE_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5QUERIES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:QUE_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:QUE_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:QUE_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:QUE_LASTSAVED), '1900-01-01')) is not null
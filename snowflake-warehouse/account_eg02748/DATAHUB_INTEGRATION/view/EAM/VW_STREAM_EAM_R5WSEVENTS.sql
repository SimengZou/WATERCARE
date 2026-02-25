CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WSEVENTS AS SELECT
                        src:WSE_BASE_EVENT::varchar AS WSE_BASE_EVENT, 
                        src:WSE_CODE::varchar AS WSE_CODE, 
                        src:WSE_DESC::varchar AS WSE_DESC, 
                        src:WSE_FILTER_PROCESSOR::varchar AS WSE_FILTER_PROCESSOR, 
                        src:WSE_LASTSAVED::datetime AS WSE_LASTSAVED, 
                        src:WSE_MEKEY::varchar AS WSE_MEKEY, 
                        src:WSE_MSG_TEMPLATE::varchar AS WSE_MSG_TEMPLATE, 
                        src:WSE_UPDATECOUNT::numeric(38, 10) AS WSE_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:WSE_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:WSE_CODE  ORDER BY 
            src:WSE_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5WSEVENTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WSE_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WSE_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WSE_LASTSAVED), '1900-01-01')) is not null
CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5LOTS AS SELECT
                        src:LOT_BREAKUPKITTRANS::varchar AS LOT_BREAKUPKITTRANS, 
                        src:LOT_BUILDKITTRANS::varchar AS LOT_BUILDKITTRANS, 
                        src:LOT_CLASS::varchar AS LOT_CLASS, 
                        src:LOT_CLASS_ORG::varchar AS LOT_CLASS_ORG, 
                        src:LOT_CODE::varchar AS LOT_CODE, 
                        src:LOT_DESC::varchar AS LOT_DESC, 
                        src:LOT_EXPIRED::datetime AS LOT_EXPIRED, 
                        src:LOT_LASTSAVED::datetime AS LOT_LASTSAVED, 
                        src:LOT_MANLOT::varchar AS LOT_MANLOT, 
                        src:LOT_ORG::varchar AS LOT_ORG, 
                        src:LOT_UPDATECOUNT::numeric(38, 10) AS LOT_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:LOT_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:LOT_CODE  ORDER BY 
            src:LOT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5LOTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LOT_EXPIRED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LOT_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LOT_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LOT_LASTSAVED), '1900-01-01')) is not null
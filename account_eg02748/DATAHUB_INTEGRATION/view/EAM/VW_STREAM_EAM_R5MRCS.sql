CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MRCS AS SELECT
                        src:MRC_CAPACITY::numeric(38, 10) AS MRC_CAPACITY, 
                        src:MRC_CLASS::varchar AS MRC_CLASS, 
                        src:MRC_CLASS_ORG::varchar AS MRC_CLASS_ORG, 
                        src:MRC_CODE::varchar AS MRC_CODE, 
                        src:MRC_CREATED::datetime AS MRC_CREATED, 
                        src:MRC_DEPOT::varchar AS MRC_DEPOT, 
                        src:MRC_DEPOT_ORG::varchar AS MRC_DEPOT_ORG, 
                        src:MRC_DESC::varchar AS MRC_DESC, 
                        src:MRC_DFLTSCREENER::varchar AS MRC_DFLTSCREENER, 
                        src:MRC_LASTSAVED::datetime AS MRC_LASTSAVED, 
                        src:MRC_NOTUSED::varchar AS MRC_NOTUSED, 
                        src:MRC_ORG::varchar AS MRC_ORG, 
                        src:MRC_SCHEDGROUP::varchar AS MRC_SCHEDGROUP, 
                        src:MRC_SEGMENTVALUE::varchar AS MRC_SEGMENTVALUE, 
                        src:MRC_STORE::varchar AS MRC_STORE, 
                        src:MRC_UPDATECOUNT::numeric(38, 10) AS MRC_UPDATECOUNT, 
                        src:MRC_UPDATED::datetime AS MRC_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:MRC_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:MRC_CODE  ORDER BY 
            src:MRC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MRCS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MRC_CAPACITY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MRC_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MRC_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MRC_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MRC_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MRC_LASTSAVED), '1900-01-01')) is not null
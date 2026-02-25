CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ALERTGENERATEWO AS SELECT
                        src:AGW_ALERT::varchar AS AGW_ALERT, 
                        src:AGW_GENERATETHROUGHFIELDID::numeric(38, 10) AS AGW_GENERATETHROUGHFIELDID, 
                        src:AGW_LASTSAVED::datetime AS AGW_LASTSAVED, 
                        src:AGW_TYPE::varchar AS AGW_TYPE, 
                        src:AGW_UPDATECOUNT::numeric(38, 10) AS AGW_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:AGW_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:AGW_ALERT  ORDER BY 
            src:AGW_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTGENERATEWO as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AGW_GENERATETHROUGHFIELDID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AGW_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AGW_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AGW_LASTSAVED), '1900-01-01')) is not null
CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ALERTIONPULSE AS SELECT
                        src:ALI_ALERT::varchar AS ALI_ALERT, 
                        src:ALI_DELAY::numeric(38, 10) AS ALI_DELAY, 
                        src:ALI_DELAYUOM::varchar AS ALI_DELAYUOM, 
                        src:ALI_DESCRIPTIONFIELDID::numeric(38, 10) AS ALI_DESCRIPTIONFIELDID, 
                        src:ALI_LASTSAVED::datetime AS ALI_LASTSAVED, 
                        src:ALI_RECIPIENTFIELDID::numeric(38, 10) AS ALI_RECIPIENTFIELDID, 
                        src:ALI_UPDATECOUNT::numeric(38, 10) AS ALI_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ALI_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ALI_ALERT  ORDER BY 
            src:ALI_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTIONPULSE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALI_DELAY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALI_DESCRIPTIONFIELDID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ALI_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALI_RECIPIENTFIELDID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALI_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ALI_LASTSAVED), '1900-01-01')) is not null
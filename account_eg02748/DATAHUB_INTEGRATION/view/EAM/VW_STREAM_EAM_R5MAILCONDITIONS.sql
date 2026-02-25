CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAILCONDITIONS AS SELECT
                        src:MAC_ANDOR::varchar AS MAC_ANDOR, 
                        src:MAC_ATTRIBPK::varchar AS MAC_ATTRIBPK, 
                        src:MAC_COLUMN::varchar AS MAC_COLUMN, 
                        src:MAC_COLUMNGRO::numeric(38, 10) AS MAC_COLUMNGRO, 
                        src:MAC_CRITERIA::varchar AS MAC_CRITERIA, 
                        src:MAC_LASTSAVED::datetime AS MAC_LASTSAVED, 
                        src:MAC_PK::varchar AS MAC_PK, 
                        src:MAC_TABLE::varchar AS MAC_TABLE, 
                        src:MAC_TEMPLATE::varchar AS MAC_TEMPLATE, 
                        src:MAC_UPDATECOUNT::numeric(38, 10) AS MAC_UPDATECOUNT, 
                        src:MAC_VALUE1::varchar AS MAC_VALUE1, 
                        src:MAC_VALUE2::varchar AS MAC_VALUE2, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:MAC_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:MAC_PK  ORDER BY 
            src:MAC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MAILCONDITIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAC_COLUMNGRO), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MAC_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAC_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MAC_LASTSAVED), '1900-01-01')) is not null
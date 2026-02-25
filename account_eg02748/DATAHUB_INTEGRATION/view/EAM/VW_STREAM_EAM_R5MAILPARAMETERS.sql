CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAILPARAMETERS AS SELECT
                        src:MAP_ATTRIBPK::varchar AS MAP_ATTRIBPK, 
                        src:MAP_COLUMN::varchar AS MAP_COLUMN, 
                        src:MAP_LASTSAVED::datetime AS MAP_LASTSAVED, 
                        src:MAP_PARAMETER::numeric(38, 10) AS MAP_PARAMETER, 
                        src:MAP_REPORTPARAMETER::numeric(38, 10) AS MAP_REPORTPARAMETER, 
                        src:MAP_TABLE::varchar AS MAP_TABLE, 
                        src:MAP_TEMPLATE::varchar AS MAP_TEMPLATE, 
                        src:MAP_UPDATECOUNT::numeric(38, 10) AS MAP_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:MAP_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:MAP_ATTRIBPK,
                src:MAP_PARAMETER  ORDER BY 
            src:MAP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MAILPARAMETERS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MAP_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAP_PARAMETER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAP_REPORTPARAMETER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAP_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MAP_LASTSAVED), '1900-01-01')) is not null
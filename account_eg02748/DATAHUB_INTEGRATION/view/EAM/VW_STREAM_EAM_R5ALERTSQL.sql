CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ALERTSQL AS SELECT
                        src:ALS_ABORTONFAILURE::varchar AS ALS_ABORTONFAILURE, 
                        src:ALS_ACTIVE::varchar AS ALS_ACTIVE, 
                        src:ALS_ALERT::varchar AS ALS_ALERT, 
                        src:ALS_COMMENT::varchar AS ALS_COMMENT, 
                        src:ALS_INCLUDEPREVIEW::varchar AS ALS_INCLUDEPREVIEW, 
                        src:ALS_LASTSAVED::datetime AS ALS_LASTSAVED, 
                        src:ALS_RTYPE::varchar AS ALS_RTYPE, 
                        src:ALS_STMT::varchar AS ALS_STMT, 
                        src:ALS_UPDATECOUNT::numeric(38, 10) AS ALS_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ALS_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ALS_ALERT,
                src:ALS_RTYPE  ORDER BY 
            src:ALS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTSQL as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ALS_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALS_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ALS_LASTSAVED), '1900-01-01')) is not null
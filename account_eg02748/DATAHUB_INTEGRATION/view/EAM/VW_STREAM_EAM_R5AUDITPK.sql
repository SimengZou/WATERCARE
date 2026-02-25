CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5AUDITPK AS SELECT
                        src:APK_COLUMN::varchar AS APK_COLUMN, 
                        src:APK_DATATYPE::varchar AS APK_DATATYPE, 
                        src:APK_LASTSAVED::datetime AS APK_LASTSAVED, 
                        src:APK_SEQNO::numeric(38, 10) AS APK_SEQNO, 
                        src:APK_TABLE::varchar AS APK_TABLE, 
                        src:APK_UPDATECOUNT::numeric(38, 10) AS APK_UPDATECOUNT, 
                        src:APK_UPDATED::datetime AS APK_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:APK_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:APK_SEQNO,
                src:APK_TABLE  ORDER BY 
            src:APK_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5AUDITPK as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:APK_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APK_SEQNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APK_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:APK_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:APK_LASTSAVED), '1900-01-01')) is not null
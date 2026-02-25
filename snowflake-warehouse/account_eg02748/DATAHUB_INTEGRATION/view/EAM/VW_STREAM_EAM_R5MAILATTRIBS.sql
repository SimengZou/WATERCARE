CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAILATTRIBS AS SELECT
                        src:MAA_ACTIVE::varchar AS MAA_ACTIVE, 
                        src:MAA_COMMENT::varchar AS MAA_COMMENT, 
                        src:MAA_DELETE::varchar AS MAA_DELETE, 
                        src:MAA_ENTEREDBY::varchar AS MAA_ENTEREDBY, 
                        src:MAA_INCLUDEATTACHMENT::varchar AS MAA_INCLUDEATTACHMENT, 
                        src:MAA_INCLUDEURL::varchar AS MAA_INCLUDEURL, 
                        src:MAA_INSERT::varchar AS MAA_INSERT, 
                        src:MAA_LASTSAVED::datetime AS MAA_LASTSAVED, 
                        src:MAA_NEWSTATUS::varchar AS MAA_NEWSTATUS, 
                        src:MAA_OLDSTATUS::varchar AS MAA_OLDSTATUS, 
                        src:MAA_PK::varchar AS MAA_PK, 
                        src:MAA_TABLE::varchar AS MAA_TABLE, 
                        src:MAA_TEMPLATE::varchar AS MAA_TEMPLATE, 
                        src:MAA_UPDATE::varchar AS MAA_UPDATE, 
                        src:MAA_UPDATECOUNT::numeric(38, 10) AS MAA_UPDATECOUNT, 
                        src:MAA_WORKFLOW::varchar AS MAA_WORKFLOW, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:MAA_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:MAA_PK  ORDER BY 
            src:MAA_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MAILATTRIBS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MAA_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAA_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MAA_LASTSAVED), '1900-01-01')) is not null
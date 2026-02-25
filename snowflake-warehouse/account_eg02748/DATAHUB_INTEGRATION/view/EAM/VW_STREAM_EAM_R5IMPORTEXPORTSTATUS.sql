CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5IMPORTEXPORTSTATUS AS SELECT
                        src:IES_COMPLETED::datetime AS IES_COMPLETED, 
                        src:IES_DATECREATED::datetime AS IES_DATECREATED, 
                        src:IES_DESC::varchar AS IES_DESC, 
                        src:IES_EMAIL::varchar AS IES_EMAIL, 
                        src:IES_EMAILSENT::varchar AS IES_EMAILSENT, 
                        src:IES_ESTTIMETOEND::datetime AS IES_ESTTIMETOEND, 
                        src:IES_ESTTIMETOSTART::datetime AS IES_ESTTIMETOSTART, 
                        src:IES_FILELOCATION::varchar AS IES_FILELOCATION, 
                        src:IES_FILESENT::varchar AS IES_FILESENT, 
                        src:IES_INCLUDEEMAIL::varchar AS IES_INCLUDEEMAIL, 
                        src:IES_LASTSAVED::datetime AS IES_LASTSAVED, 
                        src:IES_PREVIEW::varchar AS IES_PREVIEW, 
                        src:IES_RECORDCOUNT::numeric(38, 10) AS IES_RECORDCOUNT, 
                        src:IES_SESSIONID::numeric(38, 10) AS IES_SESSIONID, 
                        src:IES_STARTED::datetime AS IES_STARTED, 
                        src:IES_STATUS::varchar AS IES_STATUS, 
                        src:IES_TYPE::varchar AS IES_TYPE, 
                        src:IES_UPDATECOUNT::numeric(38, 10) AS IES_UPDATECOUNT, 
                        src:IES_USERID::varchar AS IES_USERID, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:IES_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:IES_SESSIONID  ORDER BY 
            src:IES_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5IMPORTEXPORTSTATUS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:IES_COMPLETED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:IES_DATECREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:IES_ESTTIMETOEND), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:IES_ESTTIMETOSTART), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:IES_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IES_RECORDCOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IES_SESSIONID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:IES_STARTED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IES_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:IES_LASTSAVED), '1900-01-01')) is not null
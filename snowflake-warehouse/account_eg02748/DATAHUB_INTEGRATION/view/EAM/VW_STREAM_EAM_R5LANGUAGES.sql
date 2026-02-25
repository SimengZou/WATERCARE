CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5LANGUAGES AS SELECT
                        src:LAN_AVAILABLE::varchar AS LAN_AVAILABLE, 
                        src:LAN_CLASS::varchar AS LAN_CLASS, 
                        src:LAN_CLASS_ORG::varchar AS LAN_CLASS_ORG, 
                        src:LAN_CODE::varchar AS LAN_CODE, 
                        src:LAN_DESC::varchar AS LAN_DESC, 
                        src:LAN_ERRORMESSAGE::varchar AS LAN_ERRORMESSAGE, 
                        src:LAN_INSTALLED::varchar AS LAN_INSTALLED, 
                        src:LAN_LASTCREATED::datetime AS LAN_LASTCREATED, 
                        src:LAN_LASTSAVED::datetime AS LAN_LASTSAVED, 
                        src:LAN_NOTUSED::varchar AS LAN_NOTUSED, 
                        src:LAN_PROCESSEND::datetime AS LAN_PROCESSEND, 
                        src:LAN_PROCESSSTART::datetime AS LAN_PROCESSSTART, 
                        src:LAN_RSTATUS::varchar AS LAN_RSTATUS, 
                        src:LAN_TXTTYPE::varchar AS LAN_TXTTYPE, 
                        src:LAN_UPDATECOUNT::numeric(38, 10) AS LAN_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:LAN_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:LAN_CODE  ORDER BY 
            src:LAN_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5LANGUAGES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LAN_LASTCREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LAN_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LAN_PROCESSEND), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LAN_PROCESSSTART), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LAN_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LAN_LASTSAVED), '1900-01-01')) is not null
CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5URLPARAMETERS AS SELECT
                        src:URL_ACTIVE::varchar AS URL_ACTIVE, 
                        src:URL_ALTERNATEPARAMETERNAME::varchar AS URL_ALTERNATEPARAMETERNAME, 
                        src:URL_LASTSAVED::datetime AS URL_LASTSAVED, 
                        src:URL_PARAMETERNAME::varchar AS URL_PARAMETERNAME, 
                        src:URL_PARAMETERVALUE::varchar AS URL_PARAMETERVALUE, 
                        src:URL_SYSTEM::varchar AS URL_SYSTEM, 
                        src:URL_TAB::varchar AS URL_TAB, 
                        src:URL_UPDATECOUNT::numeric(38, 10) AS URL_UPDATECOUNT, 
                        src:URL_USEFIELDVALUE::varchar AS URL_USEFIELDVALUE, 
                        src:URL_USERFUNCTION::varchar AS URL_USERFUNCTION, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:URL_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:URL_PARAMETERNAME,
                src:URL_TAB,
                src:URL_USERFUNCTION  ORDER BY 
            src:URL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5URLPARAMETERS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:URL_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:URL_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:URL_LASTSAVED), '1900-01-01')) is not null
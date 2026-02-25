CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ROLES AS SELECT
                        src:ROL_ACTIVE::varchar AS ROL_ACTIVE, 
                        src:ROL_ADVREPAUTHOR::varchar AS ROL_ADVREPAUTHOR, 
                        src:ROL_ADVREPCONSUMER::varchar AS ROL_ADVREPCONSUMER, 
                        src:ROL_ANALYTICS::varchar AS ROL_ANALYTICS, 
                        src:ROL_APPROVER::varchar AS ROL_APPROVER, 
                        src:ROL_BARCODE::varchar AS ROL_BARCODE, 
                        src:ROL_BUYER::varchar AS ROL_BUYER, 
                        src:ROL_CODE::varchar AS ROL_CODE, 
                        src:ROL_CONNECTOR::varchar AS ROL_CONNECTOR, 
                        src:ROL_DEFAULTORG::varchar AS ROL_DEFAULTORG, 
                        src:ROL_DESC::varchar AS ROL_DESC, 
                        src:ROL_EWSFIRSTFUNC::varchar AS ROL_EWSFIRSTFUNC, 
                        src:ROL_FIRSTFUNC::varchar AS ROL_FIRSTFUNC, 
                        src:ROL_GROUP::varchar AS ROL_GROUP, 
                        src:ROL_INVAPPVLIMIT::numeric(38, 10) AS ROL_INVAPPVLIMIT, 
                        src:ROL_INVAPPVLIMITNONPO::numeric(38, 10) AS ROL_INVAPPVLIMITNONPO, 
                        src:ROL_LANG::varchar AS ROL_LANG, 
                        src:ROL_LASTSAVED::datetime AS ROL_LASTSAVED, 
                        src:ROL_LOCALE::varchar AS ROL_LOCALE, 
                        src:ROL_MOBILE::varchar AS ROL_MOBILE, 
                        src:ROL_MOBILEADM::varchar AS ROL_MOBILEADM, 
                        src:ROL_MRC::varchar AS ROL_MRC, 
                        src:ROL_PICAPPVLIMIT::numeric(38, 10) AS ROL_PICAPPVLIMIT, 
                        src:ROL_PORDAPPVLIMIT::numeric(38, 10) AS ROL_PORDAPPVLIMIT, 
                        src:ROL_PORDAUTHAPPVLIMIT::numeric(38, 10) AS ROL_PORDAUTHAPPVLIMIT, 
                        src:ROL_REQAPPVLIMIT::numeric(38, 10) AS ROL_REQAPPVLIMIT, 
                        src:ROL_REQAUTHAPPVLIMIT::numeric(38, 10) AS ROL_REQAUTHAPPVLIMIT, 
                        src:ROL_REQUESTOR::varchar AS ROL_REQUESTOR, 
                        src:ROL_ROUTER::varchar AS ROL_ROUTER, 
                        src:ROL_SUCCESSMSGTIMEOUT::numeric(38, 10) AS ROL_SUCCESSMSGTIMEOUT, 
                        src:ROL_UPDATECOUNT::numeric(38, 10) AS ROL_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ROL_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ROL_CODE  ORDER BY 
            src:ROL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ROLES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROL_INVAPPVLIMIT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROL_INVAPPVLIMITNONPO), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ROL_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROL_PICAPPVLIMIT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROL_PORDAPPVLIMIT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROL_PORDAUTHAPPVLIMIT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROL_REQAPPVLIMIT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROL_REQAUTHAPPVLIMIT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROL_SUCCESSMSGTIMEOUT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROL_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ROL_LASTSAVED), '1900-01-01')) is not null
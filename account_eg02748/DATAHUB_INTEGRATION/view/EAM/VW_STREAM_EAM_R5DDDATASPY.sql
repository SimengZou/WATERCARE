CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DDDATASPY AS SELECT
                        src:DDS_AUTORUN::varchar AS DDS_AUTORUN, 
                        src:DDS_BLACKLISTVIOLATIONS::numeric(38, 10) AS DDS_BLACKLISTVIOLATIONS, 
                        src:DDS_BOTNAME::varchar AS DDS_BOTNAME, 
                        src:DDS_CLIENTROWS::numeric(38, 10) AS DDS_CLIENTROWS, 
                        src:DDS_CREATEDSTAMP::datetime AS DDS_CREATEDSTAMP, 
                        src:DDS_DDSPYFILTER::varchar AS DDS_DDSPYFILTER, 
                        src:DDS_DDSPYID::numeric(38, 10) AS DDS_DDSPYID, 
                        src:DDS_DDSPYNAME::varchar AS DDS_DDSPYNAME, 
                        src:DDS_DEFAULTFLAG::varchar AS DDS_DEFAULTFLAG, 
                        src:DDS_DISPLAYROW::numeric(38, 10) AS DDS_DISPLAYROW, 
                        src:DDS_FIELDLIST::varchar AS DDS_FIELDLIST, 
                        src:DDS_FIELDLIST_PORTLET::varchar AS DDS_FIELDLIST_PORTLET, 
                        src:DDS_FILTERSTRXML::varchar AS DDS_FILTERSTRXML, 
                        src:DDS_GLOBALDATASPY::varchar AS DDS_GLOBALDATASPY, 
                        src:DDS_GRIDID::numeric(38, 10) AS DDS_GRIDID, 
                        src:DDS_HINTS::varchar AS DDS_HINTS, 
                        src:DDS_LASTSAVED::datetime AS DDS_LASTSAVED, 
                        src:DDS_MEKEY::varchar AS DDS_MEKEY, 
                        src:DDS_ORGANIZATION::varchar AS DDS_ORGANIZATION, 
                        src:DDS_OWNER::varchar AS DDS_OWNER, 
                        src:DDS_PORTLETROWS::numeric(38, 10) AS DDS_PORTLETROWS, 
                        src:DDS_SCOPE::varchar AS DDS_SCOPE, 
                        src:DDS_SECURITYDATASPY::varchar AS DDS_SECURITYDATASPY, 
                        src:DDS_SORTSTRXML::varchar AS DDS_SORTSTRXML, 
                        src:DDS_TYPE::varchar AS DDS_TYPE, 
                        src:DDS_UPDATEABLE::varchar AS DDS_UPDATEABLE, 
                        src:DDS_UPDATEBYUSER::varchar AS DDS_UPDATEBYUSER, 
                        src:DDS_UPDATECOUNT::numeric(38, 10) AS DDS_UPDATECOUNT, 
                        src:DDS_UPDATED::datetime AS DDS_UPDATED, 
                        src:DDS_UPDATESTAMP::datetime AS DDS_UPDATESTAMP, 
                        src:DDS_USERFILTER::varchar AS DDS_USERFILTER, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:DDS_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:DDS_DDSPYID  ORDER BY 
            src:DDS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DDDATASPY as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DDS_BLACKLISTVIOLATIONS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DDS_CLIENTROWS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DDS_CREATEDSTAMP), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DDS_DDSPYID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DDS_DISPLAYROW), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DDS_GRIDID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DDS_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DDS_PORTLETROWS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DDS_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DDS_UPDATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DDS_UPDATESTAMP), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DDS_LASTSAVED), '1900-01-01')) is not null
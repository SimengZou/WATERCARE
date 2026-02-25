CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5IESREPOSITORIES AS SELECT
                        src:ENS_CODE::varchar AS ENS_CODE, 
                        src:ENS_DATECREATEDCOL::varchar AS ENS_DATECREATEDCOL, 
                        src:ENS_DATELASTCRAWL::datetime AS ENS_DATELASTCRAWL, 
                        src:ENS_DATEUPDATEDCOL::varchar AS ENS_DATEUPDATEDCOL, 
                        src:ENS_DESC::varchar AS ENS_DESC, 
                        src:ENS_INTERESTCENTER::varchar AS ENS_INTERESTCENTER, 
                        src:ENS_ISINCREMENTAL::varchar AS ENS_ISINCREMENTAL, 
                        src:ENS_ISPOPUP::varchar AS ENS_ISPOPUP, 
                        src:ENS_LASTSAVED::datetime AS ENS_LASTSAVED, 
                        src:ENS_NOTUSED::varchar AS ENS_NOTUSED, 
                        src:ENS_ORGCOL::varchar AS ENS_ORGCOL, 
                        src:ENS_TAB::varchar AS ENS_TAB, 
                        src:ENS_TABLENAME::varchar AS ENS_TABLENAME, 
                        src:ENS_TABLEPREFIX::varchar AS ENS_TABLEPREFIX, 
                        src:ENS_THUMBNAIL::varchar AS ENS_THUMBNAIL, 
                        src:ENS_UPDATECOUNT::numeric(38, 10) AS ENS_UPDATECOUNT, 
                        src:ENS_USERFUNCTION::varchar AS ENS_USERFUNCTION, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ENS_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ENS_CODE  ORDER BY 
            src:ENS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5IESREPOSITORIES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ENS_DATELASTCRAWL), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ENS_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ENS_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ENS_LASTSAVED), '1900-01-01')) is not null
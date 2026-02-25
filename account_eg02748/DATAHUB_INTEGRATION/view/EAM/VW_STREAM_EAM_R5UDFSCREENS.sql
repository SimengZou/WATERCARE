CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5UDFSCREENS AS SELECT
                        src:USC_ALLOWCOMMENTS::varchar AS USC_ALLOWCOMMENTS, 
                        src:USC_ALLOWDOCUMENTS::varchar AS USC_ALLOWDOCUMENTS, 
                        src:USC_AUTOGENERATEKEY::varchar AS USC_AUTOGENERATEKEY, 
                        src:USC_CLASS::varchar AS USC_CLASS, 
                        src:USC_CREATED::datetime AS USC_CREATED, 
                        src:USC_CREATEDBY::varchar AS USC_CREATEDBY, 
                        src:USC_DESC::varchar AS USC_DESC, 
                        src:USC_ENTITY::varchar AS USC_ENTITY, 
                        src:USC_GENERATED::varchar AS USC_GENERATED, 
                        src:USC_ISTAB::varchar AS USC_ISTAB, 
                        src:USC_LASTSAVED::datetime AS USC_LASTSAVED, 
                        src:USC_NOTUSED::varchar AS USC_NOTUSED, 
                        src:USC_ORGSECURITY::varchar AS USC_ORGSECURITY, 
                        src:USC_PARENTSCREEN::varchar AS USC_PARENTSCREEN, 
                        src:USC_SCREENNAME::varchar AS USC_SCREENNAME, 
                        src:USC_STATUSENTITY::varchar AS USC_STATUSENTITY, 
                        src:USC_TABLENAME::varchar AS USC_TABLENAME, 
                        src:USC_TYPEENTITY::varchar AS USC_TYPEENTITY, 
                        src:USC_UPDATECOUNT::numeric(38, 10) AS USC_UPDATECOUNT, 
                        src:USC_UPDATED::datetime AS USC_UPDATED, 
                        src:USC_UPDATEDBY::varchar AS USC_UPDATEDBY, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:USC_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:USC_SCREENNAME  ORDER BY 
            src:USC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5UDFSCREENS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:USC_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:USC_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USC_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:USC_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:USC_LASTSAVED), '1900-01-01')) is not null
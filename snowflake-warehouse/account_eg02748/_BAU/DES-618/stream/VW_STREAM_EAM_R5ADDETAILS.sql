CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ADDETAILS
            AS
            SELECT
                src:"ADD_CODE"::varchar AS ADD_CODE, 
                src:"ADD_CREATED"::datetime AS ADD_CREATED, 
                src:"ADD_ENTITY"::varchar AS ADD_ENTITY, 
                src:"ADD_LANG"::varchar AS ADD_LANG, 
                src:"ADD_LASTSAVED"::datetime AS ADD_LASTSAVED,  
                src:"ADD_LASTSAVED"::datetime as ETL_LASTSAVED,
                src:"ADD_LINE"::numeric(38, 10) AS ADD_LINE, 
                src:"ADD_PRINT"::varchar AS ADD_PRINT, 
                src:"ADD_RENTITY"::varchar AS ADD_RENTITY, 
                src:"ADD_RTYPE"::varchar AS ADD_RTYPE, 
                src:"ADD_TEXT"::varchar AS ADD_TEXT, 
                src:"ADD_TYPE"::varchar AS ADD_TYPE, 
                src:"ADD_UPDATECOUNT"::numeric(38, 10) AS ADD_UPDATECOUNT, 
                src:"ADD_UPDATED"::datetime AS ADD_UPDATED, 
                src:"ADD_UPDUSER"::varchar AS ADD_UPDUSER, 
                src:"ADD_USER"::varchar AS ADD_USER, 
                src:"_DELETED"::boolean AS _DELETED, 
                src:"_DELETED"::BOOLEAN as ETL_DELETED, 
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
     
                            
                src:"ADD_CODE"::varchar,
                src:"ADD_LANG"::varchar,
                src:"ADD_LINE"::numeric(38, 10),
                src:"ADD_RENTITY"::varchar,
                src:"ADD_TYPE"::varchar,
                src:"ADD_LASTSAVED"::datetime
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:"ADD_CODE"::varchar,
                src:"ADD_LANG"::varchar,
                src:"ADD_LINE"::numeric(38, 10),
                src:"ADD_RENTITY"::varchar,
                src:"ADD_TYPE"::varchar  ORDER BY 
                src:"ADD_LASTSAVED"::datetime desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ADDETAILS as strm

)
    WHERE
    ROWNUMBER=1)
                ; 
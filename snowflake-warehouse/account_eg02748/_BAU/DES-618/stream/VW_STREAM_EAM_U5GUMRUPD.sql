CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_U5GUMRUPD
            AS
            SELECT
                src:"CREATED"::datetime AS CREATED, 
                src:"CREATEDBY"::varchar AS CREATEDBY, 
                src:"LASTSAVED"::datetime AS LASTSAVED,  
                src:"LASTSAVED"::datetime as ETL_LASTSAVED,
                src:"MRU_CONTRACTORCODE"::varchar AS MRU_CONTRACTORCODE, 
                src:"MRU_EVENT"::varchar AS MRU_EVENT, 
                src:"MRU_NUMBER"::varchar AS MRU_NUMBER, 
                src:"MRU_REASON"::varchar AS MRU_REASON, 
                src:"MRU_TRANSID"::varchar AS MRU_TRANSID, 
                src:"MRU_UPDATEDATE"::datetime AS MRU_UPDATEDATE, 
                src:"MRU_UPDATETYPE"::varchar AS MRU_UPDATETYPE, 
                src:"UPDATECOUNT"::numeric(38, 10) AS UPDATECOUNT, 
                src:"UPDATED"::datetime AS UPDATED, 
                src:"UPDATEDBY"::varchar AS UPDATEDBY, 
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
     
                            
                src:"MRU_TRANSID"::varchar,
                src:"LASTSAVED"::datetime
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:"MRU_TRANSID"::varchar  ORDER BY 
                src:"LASTSAVED"::datetime desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_U5GUMRUPD as strm

)
    WHERE
    ROWNUMBER=1)
                ; 
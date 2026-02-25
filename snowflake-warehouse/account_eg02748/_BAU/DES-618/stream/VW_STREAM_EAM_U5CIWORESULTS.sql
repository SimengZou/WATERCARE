CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_U5CIWORESULTS
            AS
            SELECT
                src:"CIR_CODE"::varchar AS CIR_CODE, 
                src:"CIR_DETAIL"::varchar AS CIR_DETAIL, 
                src:"CIR_OBSDATE"::datetime AS CIR_OBSDATE, 
                src:"CIR_OBSDESCRIPTION"::varchar AS CIR_OBSDESCRIPTION, 
                src:"CIR_OBSUOM"::varchar AS CIR_OBSUOM, 
                src:"CIR_OBSVALUE"::varchar AS CIR_OBSVALUE, 
                src:"CIR_SEQUENCE"::numeric(38, 10) AS CIR_SEQUENCE, 
                src:"CIR_TRANSID"::varchar AS CIR_TRANSID, 
                src:"CIR_TYPE"::varchar AS CIR_TYPE, 
                src:"CIR_WORKORDERNUM"::varchar AS CIR_WORKORDERNUM, 
                src:"CREATED"::datetime AS CREATED, 
                src:"CREATEDBY"::varchar AS CREATEDBY, 
                src:"LASTSAVED"::datetime AS LASTSAVED,  
                src:"LASTSAVED"::datetime as ETL_LASTSAVED,
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
     
                            
                src:"CIR_SEQUENCE"::numeric(38, 10),
                src:"LASTSAVED"::datetime
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:"CIR_SEQUENCE"::numeric(38, 10)  ORDER BY 
                src:"LASTSAVED"::datetime desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_U5CIWORESULTS as strm

)
    WHERE
    ROWNUMBER=1)
                ; 
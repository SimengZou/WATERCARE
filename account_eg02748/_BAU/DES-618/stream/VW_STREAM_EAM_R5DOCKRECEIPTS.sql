CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DOCKRECEIPTS
            AS
            SELECT
                src:"DCK_CLASS"::varchar AS DCK_CLASS, 
                src:"DCK_CLASS_ORG"::varchar AS DCK_CLASS_ORG, 
                src:"DCK_CODE"::varchar AS DCK_CODE, 
                src:"DCK_CREATED"::datetime AS DCK_CREATED, 
                src:"DCK_DESC"::varchar AS DCK_DESC, 
                src:"DCK_DOCKID"::varchar AS DCK_DOCKID, 
                src:"DCK_FREIGHT"::varchar AS DCK_FREIGHT, 
                src:"DCK_LASTSAVED"::datetime AS DCK_LASTSAVED,  
                src:"DCK_LASTSAVED"::datetime as ETL_LASTSAVED,
                src:"DCK_LOCATION"::varchar AS DCK_LOCATION, 
                src:"DCK_MRC"::varchar AS DCK_MRC, 
                src:"DCK_ORDER"::varchar AS DCK_ORDER, 
                src:"DCK_ORDER_ORG"::varchar AS DCK_ORDER_ORG, 
                src:"DCK_ORG"::varchar AS DCK_ORG, 
                src:"DCK_PACKSLIP"::varchar AS DCK_PACKSLIP, 
                src:"DCK_RECEIVER"::varchar AS DCK_RECEIVER, 
                src:"DCK_RECVDATE"::datetime AS DCK_RECVDATE, 
                src:"DCK_REFERENCE"::varchar AS DCK_REFERENCE, 
                src:"DCK_RETRIEVEALL"::varchar AS DCK_RETRIEVEALL, 
                src:"DCK_RSTATUS"::varchar AS DCK_RSTATUS, 
                src:"DCK_SHIPVIA"::varchar AS DCK_SHIPVIA, 
                src:"DCK_STATUS"::varchar AS DCK_STATUS, 
                src:"DCK_SUPPLIER"::varchar AS DCK_SUPPLIER, 
                src:"DCK_SUPPLIER_ORG"::varchar AS DCK_SUPPLIER_ORG, 
                src:"DCK_UPDATECOUNT"::numeric(38, 10) AS DCK_UPDATECOUNT, 
                src:"DCK_UPDATED"::datetime AS DCK_UPDATED, 
                src:"DCK_UPDUSER"::varchar AS DCK_UPDUSER, 
                src:"DCK_USER"::varchar AS DCK_USER, 
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
     
                            
                src:"DCK_CODE"::varchar,
                src:"DCK_LASTSAVED"::datetime
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:"DCK_CODE"::varchar  ORDER BY 
                src:"DCK_LASTSAVED"::datetime desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DOCKRECEIPTS as strm

)
    WHERE
    ROWNUMBER=1)
                ; 
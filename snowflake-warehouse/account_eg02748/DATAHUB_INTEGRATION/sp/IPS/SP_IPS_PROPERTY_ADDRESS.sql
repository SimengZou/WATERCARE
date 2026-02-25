create or replace procedure DATAHUB_INTEGRATION.SP_IPS_PROPERTY_ADDRESS()
    returns varchar not null
                language javascript
                as
                $$

//  Variables

var result = "";                                    // return status of this proc call
const process_name = Object.keys(this)[0];          // name of currently executing process
var number_of_rows_inserted = 0;                             // track number of rows we have inserted
var number_of_rows_updated = 0;                             // track number of rows we have updated


//  Step 1.

//  Start execution - log start

log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('Insert','" + process_name + "','Running','Started process execution.', 0,0,0);";
snowflake.execute({sqlText: log_sql_command});

snowflake.execute( {sqlText: "begin transaction;"} );
try
    {
        var sql_command = `
                            INSERT INTO LANDING_ERROR.IPS_RAW_ERROR
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_PROPERTY_ADDRESS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_PROPERTY_ADDRESS as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDRESSSTATUS, 
            strm.ADDRKEY, 
            strm.ALLOWDUPLICATESERVICES, 
            strm.AREA, 
            strm.BLOCK, 
            strm.CASSISVALID, 
            strm.CASSVALIDATIONDESC, 
            strm.CASSVALIDATIONDT, 
            strm.CASSVALIDATIONSTATUS, 
            strm.CITYLIMITS, 
            strm.CURRADDR, 
            strm.DELETED, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.FULLSTNO, 
            strm.GPSX, 
            strm.GPSY, 
            strm.GPSZ, 
            strm.INOUTSERVICEAREA, 
            strm.LEGALOWNER, 
            strm.LOT, 
            strm.MANAGEMENTGROUP, 
            strm.MAPNO, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NZADDRTYPE, 
            strm.NZCOMPDIR, 
            strm.NZFLAT, 
            strm.NZHNOHI, 
            strm.NZHNOSORTAS, 
            strm.NZHNOSORTASHI, 
            strm.NZHOUSENO, 
            strm.NZPOSTCODE, 
            strm.NZST2NAME, 
            strm.NZST2TYPE, 
            strm.NZST3NAME, 
            strm.NZST3TYPE, 
            strm.NZSTATE, 
            strm.NZSTNAME, 
            strm.NZSTTYPE, 
            strm.NZSUBURB, 
            strm.OPTA, 
            strm.OPTB, 
            strm.OPTC, 
            strm.OPTD, 
            strm.PROPERTYUSE, 
            strm.SUBDIVCODE, 
            strm.SUBDIVDESC, 
            strm.TOWNSHIP, 
            strm.VARIATION_ID, 
            strm.VERSION, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ADDRKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_PROPERTY_ADDRESS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ADDRKEY=src.ADDRKEY) OR (target.ADDRKEY IS NULL AND src.ADDRKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDRESSSTATUS=src.ADDRESSSTATUS, 
                    target.ADDRKEY=src.ADDRKEY, 
                    target.ALLOWDUPLICATESERVICES=src.ALLOWDUPLICATESERVICES, 
                    target.AREA=src.AREA, 
                    target.BLOCK=src.BLOCK, 
                    target.CASSISVALID=src.CASSISVALID, 
                    target.CASSVALIDATIONDESC=src.CASSVALIDATIONDESC, 
                    target.CASSVALIDATIONDT=src.CASSVALIDATIONDT, 
                    target.CASSVALIDATIONSTATUS=src.CASSVALIDATIONSTATUS, 
                    target.CITYLIMITS=src.CITYLIMITS, 
                    target.CURRADDR=src.CURRADDR, 
                    target.DELETED=src.DELETED, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.FULLSTNO=src.FULLSTNO, 
                    target.GPSX=src.GPSX, 
                    target.GPSY=src.GPSY, 
                    target.GPSZ=src.GPSZ, 
                    target.INOUTSERVICEAREA=src.INOUTSERVICEAREA, 
                    target.LEGALOWNER=src.LEGALOWNER, 
                    target.LOT=src.LOT, 
                    target.MANAGEMENTGROUP=src.MANAGEMENTGROUP, 
                    target.MAPNO=src.MAPNO, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NZADDRTYPE=src.NZADDRTYPE, 
                    target.NZCOMPDIR=src.NZCOMPDIR, 
                    target.NZFLAT=src.NZFLAT, 
                    target.NZHNOHI=src.NZHNOHI, 
                    target.NZHNOSORTAS=src.NZHNOSORTAS, 
                    target.NZHNOSORTASHI=src.NZHNOSORTASHI, 
                    target.NZHOUSENO=src.NZHOUSENO, 
                    target.NZPOSTCODE=src.NZPOSTCODE, 
                    target.NZST2NAME=src.NZST2NAME, 
                    target.NZST2TYPE=src.NZST2TYPE, 
                    target.NZST3NAME=src.NZST3NAME, 
                    target.NZST3TYPE=src.NZST3TYPE, 
                    target.NZSTATE=src.NZSTATE, 
                    target.NZSTNAME=src.NZSTNAME, 
                    target.NZSTTYPE=src.NZSTTYPE, 
                    target.NZSUBURB=src.NZSUBURB, 
                    target.OPTA=src.OPTA, 
                    target.OPTB=src.OPTB, 
                    target.OPTC=src.OPTC, 
                    target.OPTD=src.OPTD, 
                    target.PROPERTYUSE=src.PROPERTYUSE, 
                    target.SUBDIVCODE=src.SUBDIVCODE, 
                    target.SUBDIVDESC=src.SUBDIVDESC, 
                    target.TOWNSHIP=src.TOWNSHIP, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VERSION=src.VERSION, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    ADDRESSSTATUS, 
                    ADDRKEY, 
                    ALLOWDUPLICATESERVICES, 
                    AREA, 
                    BLOCK, 
                    CASSISVALID, 
                    CASSVALIDATIONDESC, 
                    CASSVALIDATIONDT, 
                    CASSVALIDATIONSTATUS, 
                    CITYLIMITS, 
                    CURRADDR, 
                    DELETED, 
                    EFFDATE, 
                    EXPDATE, 
                    FULLSTNO, 
                    GPSX, 
                    GPSY, 
                    GPSZ, 
                    INOUTSERVICEAREA, 
                    LEGALOWNER, 
                    LOT, 
                    MANAGEMENTGROUP, 
                    MAPNO, 
                    MODBY, 
                    MODDTTM, 
                    NZADDRTYPE, 
                    NZCOMPDIR, 
                    NZFLAT, 
                    NZHNOHI, 
                    NZHNOSORTAS, 
                    NZHNOSORTASHI, 
                    NZHOUSENO, 
                    NZPOSTCODE, 
                    NZST2NAME, 
                    NZST2TYPE, 
                    NZST3NAME, 
                    NZST3TYPE, 
                    NZSTATE, 
                    NZSTNAME, 
                    NZSTTYPE, 
                    NZSUBURB, 
                    OPTA, 
                    OPTB, 
                    OPTC, 
                    OPTD, 
                    PROPERTYUSE, 
                    SUBDIVCODE, 
                    SUBDIVDESC, 
                    TOWNSHIP, 
                    VARIATION_ID, 
                    VERSION, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDRESSSTATUS, 
                    src.ADDRKEY, 
                    src.ALLOWDUPLICATESERVICES, 
                    src.AREA, 
                    src.BLOCK, 
                    src.CASSISVALID, 
                    src.CASSVALIDATIONDESC, 
                    src.CASSVALIDATIONDT, 
                    src.CASSVALIDATIONSTATUS, 
                    src.CITYLIMITS, 
                    src.CURRADDR, 
                    src.DELETED, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.FULLSTNO, 
                    src.GPSX, 
                    src.GPSY, 
                    src.GPSZ, 
                    src.INOUTSERVICEAREA, 
                    src.LEGALOWNER, 
                    src.LOT, 
                    src.MANAGEMENTGROUP, 
                    src.MAPNO, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NZADDRTYPE, 
                    src.NZCOMPDIR, 
                    src.NZFLAT, 
                    src.NZHNOHI, 
                    src.NZHNOSORTAS, 
                    src.NZHNOSORTASHI, 
                    src.NZHOUSENO, 
                    src.NZPOSTCODE, 
                    src.NZST2NAME, 
                    src.NZST2TYPE, 
                    src.NZST3NAME, 
                    src.NZST3TYPE, 
                    src.NZSTATE, 
                    src.NZSTNAME, 
                    src.NZSTTYPE, 
                    src.NZSUBURB, 
                    src.OPTA, 
                    src.OPTB, 
                    src.OPTC, 
                    src.OPTD, 
                    src.PROPERTYUSE, 
                    src.SUBDIVCODE, 
                    src.SUBDIVDESC, 
                    src.TOWNSHIP, 
                    src.VARIATION_ID, 
                    src.VERSION,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_PROPERTY_ADDRESS as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDRESSSTATUS, 
            strm.ADDRKEY, 
            strm.ALLOWDUPLICATESERVICES, 
            strm.AREA, 
            strm.BLOCK, 
            strm.CASSISVALID, 
            strm.CASSVALIDATIONDESC, 
            strm.CASSVALIDATIONDT, 
            strm.CASSVALIDATIONSTATUS, 
            strm.CITYLIMITS, 
            strm.CURRADDR, 
            strm.DELETED, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.FULLSTNO, 
            strm.GPSX, 
            strm.GPSY, 
            strm.GPSZ, 
            strm.INOUTSERVICEAREA, 
            strm.LEGALOWNER, 
            strm.LOT, 
            strm.MANAGEMENTGROUP, 
            strm.MAPNO, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NZADDRTYPE, 
            strm.NZCOMPDIR, 
            strm.NZFLAT, 
            strm.NZHNOHI, 
            strm.NZHNOSORTAS, 
            strm.NZHNOSORTASHI, 
            strm.NZHOUSENO, 
            strm.NZPOSTCODE, 
            strm.NZST2NAME, 
            strm.NZST2TYPE, 
            strm.NZST3NAME, 
            strm.NZST3TYPE, 
            strm.NZSTATE, 
            strm.NZSTNAME, 
            strm.NZSTTYPE, 
            strm.NZSUBURB, 
            strm.OPTA, 
            strm.OPTB, 
            strm.OPTC, 
            strm.OPTD, 
            strm.PROPERTYUSE, 
            strm.SUBDIVCODE, 
            strm.SUBDIVDESC, 
            strm.TOWNSHIP, 
            strm.VARIATION_ID, 
            strm.VERSION, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ADDRKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_PROPERTY_ADDRESS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ADDRKEY=src.ADDRKEY) OR (target.ADDRKEY IS NULL AND src.ADDRKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDRESSSTATUS=src.ADDRESSSTATUS, 
                    target.ADDRKEY=src.ADDRKEY, 
                    target.ALLOWDUPLICATESERVICES=src.ALLOWDUPLICATESERVICES, 
                    target.AREA=src.AREA, 
                    target.BLOCK=src.BLOCK, 
                    target.CASSISVALID=src.CASSISVALID, 
                    target.CASSVALIDATIONDESC=src.CASSVALIDATIONDESC, 
                    target.CASSVALIDATIONDT=src.CASSVALIDATIONDT, 
                    target.CASSVALIDATIONSTATUS=src.CASSVALIDATIONSTATUS, 
                    target.CITYLIMITS=src.CITYLIMITS, 
                    target.CURRADDR=src.CURRADDR, 
                    target.DELETED=src.DELETED, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.FULLSTNO=src.FULLSTNO, 
                    target.GPSX=src.GPSX, 
                    target.GPSY=src.GPSY, 
                    target.GPSZ=src.GPSZ, 
                    target.INOUTSERVICEAREA=src.INOUTSERVICEAREA, 
                    target.LEGALOWNER=src.LEGALOWNER, 
                    target.LOT=src.LOT, 
                    target.MANAGEMENTGROUP=src.MANAGEMENTGROUP, 
                    target.MAPNO=src.MAPNO, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NZADDRTYPE=src.NZADDRTYPE, 
                    target.NZCOMPDIR=src.NZCOMPDIR, 
                    target.NZFLAT=src.NZFLAT, 
                    target.NZHNOHI=src.NZHNOHI, 
                    target.NZHNOSORTAS=src.NZHNOSORTAS, 
                    target.NZHNOSORTASHI=src.NZHNOSORTASHI, 
                    target.NZHOUSENO=src.NZHOUSENO, 
                    target.NZPOSTCODE=src.NZPOSTCODE, 
                    target.NZST2NAME=src.NZST2NAME, 
                    target.NZST2TYPE=src.NZST2TYPE, 
                    target.NZST3NAME=src.NZST3NAME, 
                    target.NZST3TYPE=src.NZST3TYPE, 
                    target.NZSTATE=src.NZSTATE, 
                    target.NZSTNAME=src.NZSTNAME, 
                    target.NZSTTYPE=src.NZSTTYPE, 
                    target.NZSUBURB=src.NZSUBURB, 
                    target.OPTA=src.OPTA, 
                    target.OPTB=src.OPTB, 
                    target.OPTC=src.OPTC, 
                    target.OPTD=src.OPTD, 
                    target.PROPERTYUSE=src.PROPERTYUSE, 
                    target.SUBDIVCODE=src.SUBDIVCODE, 
                    target.SUBDIVDESC=src.SUBDIVDESC, 
                    target.TOWNSHIP=src.TOWNSHIP, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VERSION=src.VERSION, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, ADDRESSSTATUS, ADDRKEY, ALLOWDUPLICATESERVICES, AREA, BLOCK, CASSISVALID, CASSVALIDATIONDESC, CASSVALIDATIONDT, CASSVALIDATIONSTATUS, CITYLIMITS, CURRADDR, DELETED, EFFDATE, EXPDATE, FULLSTNO, GPSX, GPSY, GPSZ, INOUTSERVICEAREA, LEGALOWNER, LOT, MANAGEMENTGROUP, MAPNO, MODBY, MODDTTM, NZADDRTYPE, NZCOMPDIR, NZFLAT, NZHNOHI, NZHNOSORTAS, NZHNOSORTASHI, NZHOUSENO, NZPOSTCODE, NZST2NAME, NZST2TYPE, NZST3NAME, NZST3TYPE, NZSTATE, NZSTNAME, NZSTTYPE, NZSUBURB, OPTA, OPTB, OPTC, OPTD, PROPERTYUSE, SUBDIVCODE, SUBDIVDESC, TOWNSHIP, VARIATION_ID, VERSION, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDRESSSTATUS, 
                    src.ADDRKEY, 
                    src.ALLOWDUPLICATESERVICES, 
                    src.AREA, 
                    src.BLOCK, 
                    src.CASSISVALID, 
                    src.CASSVALIDATIONDESC, 
                    src.CASSVALIDATIONDT, 
                    src.CASSVALIDATIONSTATUS, 
                    src.CITYLIMITS, 
                    src.CURRADDR, 
                    src.DELETED, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.FULLSTNO, 
                    src.GPSX, 
                    src.GPSY, 
                    src.GPSZ, 
                    src.INOUTSERVICEAREA, 
                    src.LEGALOWNER, 
                    src.LOT, 
                    src.MANAGEMENTGROUP, 
                    src.MAPNO, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NZADDRTYPE, 
                    src.NZCOMPDIR, 
                    src.NZFLAT, 
                    src.NZHNOHI, 
                    src.NZHNOSORTAS, 
                    src.NZHNOSORTASHI, 
                    src.NZHOUSENO, 
                    src.NZPOSTCODE, 
                    src.NZST2NAME, 
                    src.NZST2TYPE, 
                    src.NZST3NAME, 
                    src.NZST3TYPE, 
                    src.NZSTATE, 
                    src.NZSTNAME, 
                    src.NZSTTYPE, 
                    src.NZSUBURB, 
                    src.OPTA, 
                    src.OPTB, 
                    src.OPTC, 
                    src.OPTD, 
                    src.PROPERTYUSE, 
                    src.SUBDIVCODE, 
                    src.SUBDIVDESC, 
                    src.TOWNSHIP, 
                    src.VARIATION_ID, 
                    src.VERSION,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename,
                    case when ETL_DELETED=TRUE then TRUE else FALSE end,
                    ETL_DELETED);
    `
} );

                    
//  Get number of rows inserted
        
        var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
        var rs = sqlStmt.execute();
        var number_of_rows_inserted = sqlStmt.getNumRowsInserted();
        var number_of_rows_updated =  sqlStmt.getNumRowsUpdated();
                    

        
    snowflake.execute ({sqlText: "commit"});

    log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name + "','Success','Completed process execution.', 0 ," + number_of_rows_inserted.toString()+ "," +  number_of_rows_updated.toString() + ");";
    snowflake.execute({sqlText: log_sql_command});

    result = "Success"; 

    }       

    catch (err)  {
        snowflake.execute( {sqlText: "rollback;"} )
        var clean_error_msg = err.message.replace(/[^\w\s]/gi, '');
    //  Create a log entry to say INSERT STATEMENT failed
    
    log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name + "','Failed','MERGE Failed. Error:" + err.code.toString()+" : "+ clean_error_msg  + "', 0 ,0,0);";
    snowflake.execute({sqlText: log_sql_command});
        ;
        throw "Failed: " + err.message;   // Return a success/error indicator.
        }
    return result;
    $$;
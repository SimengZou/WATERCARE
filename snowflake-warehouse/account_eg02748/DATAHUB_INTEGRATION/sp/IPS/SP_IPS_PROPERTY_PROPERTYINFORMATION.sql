create or replace procedure DATAHUB_INTEGRATION.SP_IPS_PROPERTY_PROPERTYINFORMATION()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_PROPERTY_PROPERTYINFORMATION_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_PROPERTY_PROPERTYINFORMATION as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AREA, 
            strm.BLOCK, 
            strm.BOROUGH, 
            strm.CENSUSTRACT, 
            strm.CITYOWNED, 
            strm.COUNTY, 
            strm.CURRPROP, 
            strm.DELETED, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.HEALTHAREA, 
            strm.LANDMARK, 
            strm.LEGALOWNER, 
            strm.LOFT, 
            strm.LOT, 
            strm.MAPNUMBER, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NOTES, 
            strm.PLANMEDIA, 
            strm.PROPID, 
            strm.PROPKEY, 
            strm.PROPNAME, 
            strm.PROPNOTE, 
            strm.PROPTYPE, 
            strm.SPECIALPLACENAME, 
            strm.STATUS, 
            strm.SUBDIVDESC, 
            strm.SUBDIVISION, 
            strm.TAXBLOCK, 
            strm.TAXEXEMPT, 
            strm.TOWNSHIP, 
            strm.VACANT, 
            strm.VARIATION_ID, 
            strm.VERSION, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PROPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_PROPERTY_PROPERTYINFORMATION as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PROPKEY=src.PROPKEY) OR (target.PROPKEY IS NULL AND src.PROPKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AREA=src.AREA, 
                    target.BLOCK=src.BLOCK, 
                    target.BOROUGH=src.BOROUGH, 
                    target.CENSUSTRACT=src.CENSUSTRACT, 
                    target.CITYOWNED=src.CITYOWNED, 
                    target.COUNTY=src.COUNTY, 
                    target.CURRPROP=src.CURRPROP, 
                    target.DELETED=src.DELETED, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.HEALTHAREA=src.HEALTHAREA, 
                    target.LANDMARK=src.LANDMARK, 
                    target.LEGALOWNER=src.LEGALOWNER, 
                    target.LOFT=src.LOFT, 
                    target.LOT=src.LOT, 
                    target.MAPNUMBER=src.MAPNUMBER, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NOTES=src.NOTES, 
                    target.PLANMEDIA=src.PLANMEDIA, 
                    target.PROPID=src.PROPID, 
                    target.PROPKEY=src.PROPKEY, 
                    target.PROPNAME=src.PROPNAME, 
                    target.PROPNOTE=src.PROPNOTE, 
                    target.PROPTYPE=src.PROPTYPE, 
                    target.SPECIALPLACENAME=src.SPECIALPLACENAME, 
                    target.STATUS=src.STATUS, 
                    target.SUBDIVDESC=src.SUBDIVDESC, 
                    target.SUBDIVISION=src.SUBDIVISION, 
                    target.TAXBLOCK=src.TAXBLOCK, 
                    target.TAXEXEMPT=src.TAXEXEMPT, 
                    target.TOWNSHIP=src.TOWNSHIP, 
                    target.VACANT=src.VACANT, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VERSION=src.VERSION, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    AREA, 
                    BLOCK, 
                    BOROUGH, 
                    CENSUSTRACT, 
                    CITYOWNED, 
                    COUNTY, 
                    CURRPROP, 
                    DELETED, 
                    EFFDATE, 
                    EXPDATE, 
                    HEALTHAREA, 
                    LANDMARK, 
                    LEGALOWNER, 
                    LOFT, 
                    LOT, 
                    MAPNUMBER, 
                    MODBY, 
                    MODDTTM, 
                    NOTES, 
                    PLANMEDIA, 
                    PROPID, 
                    PROPKEY, 
                    PROPNAME, 
                    PROPNOTE, 
                    PROPTYPE, 
                    SPECIALPLACENAME, 
                    STATUS, 
                    SUBDIVDESC, 
                    SUBDIVISION, 
                    TAXBLOCK, 
                    TAXEXEMPT, 
                    TOWNSHIP, 
                    VACANT, 
                    VARIATION_ID, 
                    VERSION, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AREA, 
                    src.BLOCK, 
                    src.BOROUGH, 
                    src.CENSUSTRACT, 
                    src.CITYOWNED, 
                    src.COUNTY, 
                    src.CURRPROP, 
                    src.DELETED, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.HEALTHAREA, 
                    src.LANDMARK, 
                    src.LEGALOWNER, 
                    src.LOFT, 
                    src.LOT, 
                    src.MAPNUMBER, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NOTES, 
                    src.PLANMEDIA, 
                    src.PROPID, 
                    src.PROPKEY, 
                    src.PROPNAME, 
                    src.PROPNOTE, 
                    src.PROPTYPE, 
                    src.SPECIALPLACENAME, 
                    src.STATUS, 
                    src.SUBDIVDESC, 
                    src.SUBDIVISION, 
                    src.TAXBLOCK, 
                    src.TAXEXEMPT, 
                    src.TOWNSHIP, 
                    src.VACANT, 
                    src.VARIATION_ID, 
                    src.VERSION,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_PROPERTY_PROPERTYINFORMATION as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AREA, 
            strm.BLOCK, 
            strm.BOROUGH, 
            strm.CENSUSTRACT, 
            strm.CITYOWNED, 
            strm.COUNTY, 
            strm.CURRPROP, 
            strm.DELETED, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.HEALTHAREA, 
            strm.LANDMARK, 
            strm.LEGALOWNER, 
            strm.LOFT, 
            strm.LOT, 
            strm.MAPNUMBER, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NOTES, 
            strm.PLANMEDIA, 
            strm.PROPID, 
            strm.PROPKEY, 
            strm.PROPNAME, 
            strm.PROPNOTE, 
            strm.PROPTYPE, 
            strm.SPECIALPLACENAME, 
            strm.STATUS, 
            strm.SUBDIVDESC, 
            strm.SUBDIVISION, 
            strm.TAXBLOCK, 
            strm.TAXEXEMPT, 
            strm.TOWNSHIP, 
            strm.VACANT, 
            strm.VARIATION_ID, 
            strm.VERSION, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PROPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_PROPERTY_PROPERTYINFORMATION as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PROPKEY=src.PROPKEY) OR (target.PROPKEY IS NULL AND src.PROPKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AREA=src.AREA, 
                    target.BLOCK=src.BLOCK, 
                    target.BOROUGH=src.BOROUGH, 
                    target.CENSUSTRACT=src.CENSUSTRACT, 
                    target.CITYOWNED=src.CITYOWNED, 
                    target.COUNTY=src.COUNTY, 
                    target.CURRPROP=src.CURRPROP, 
                    target.DELETED=src.DELETED, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.HEALTHAREA=src.HEALTHAREA, 
                    target.LANDMARK=src.LANDMARK, 
                    target.LEGALOWNER=src.LEGALOWNER, 
                    target.LOFT=src.LOFT, 
                    target.LOT=src.LOT, 
                    target.MAPNUMBER=src.MAPNUMBER, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NOTES=src.NOTES, 
                    target.PLANMEDIA=src.PLANMEDIA, 
                    target.PROPID=src.PROPID, 
                    target.PROPKEY=src.PROPKEY, 
                    target.PROPNAME=src.PROPNAME, 
                    target.PROPNOTE=src.PROPNOTE, 
                    target.PROPTYPE=src.PROPTYPE, 
                    target.SPECIALPLACENAME=src.SPECIALPLACENAME, 
                    target.STATUS=src.STATUS, 
                    target.SUBDIVDESC=src.SUBDIVDESC, 
                    target.SUBDIVISION=src.SUBDIVISION, 
                    target.TAXBLOCK=src.TAXBLOCK, 
                    target.TAXEXEMPT=src.TAXEXEMPT, 
                    target.TOWNSHIP=src.TOWNSHIP, 
                    target.VACANT=src.VACANT, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VERSION=src.VERSION, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, AREA, BLOCK, BOROUGH, CENSUSTRACT, CITYOWNED, COUNTY, CURRPROP, DELETED, EFFDATE, EXPDATE, HEALTHAREA, LANDMARK, LEGALOWNER, LOFT, LOT, MAPNUMBER, MODBY, MODDTTM, NOTES, PLANMEDIA, PROPID, PROPKEY, PROPNAME, PROPNOTE, PROPTYPE, SPECIALPLACENAME, STATUS, SUBDIVDESC, SUBDIVISION, TAXBLOCK, TAXEXEMPT, TOWNSHIP, VACANT, VARIATION_ID, VERSION, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AREA, 
                    src.BLOCK, 
                    src.BOROUGH, 
                    src.CENSUSTRACT, 
                    src.CITYOWNED, 
                    src.COUNTY, 
                    src.CURRPROP, 
                    src.DELETED, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.HEALTHAREA, 
                    src.LANDMARK, 
                    src.LEGALOWNER, 
                    src.LOFT, 
                    src.LOT, 
                    src.MAPNUMBER, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NOTES, 
                    src.PLANMEDIA, 
                    src.PROPID, 
                    src.PROPKEY, 
                    src.PROPNAME, 
                    src.PROPNOTE, 
                    src.PROPTYPE, 
                    src.SPECIALPLACENAME, 
                    src.STATUS, 
                    src.SUBDIVDESC, 
                    src.SUBDIVISION, 
                    src.TAXBLOCK, 
                    src.TAXEXEMPT, 
                    src.TOWNSHIP, 
                    src.VACANT, 
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
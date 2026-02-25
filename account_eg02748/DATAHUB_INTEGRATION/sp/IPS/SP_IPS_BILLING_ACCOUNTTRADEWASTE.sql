create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_ACCOUNTTRADEWASTE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_ACCOUNTTRADEWASTE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_ACCOUNTTRADEWASTE as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTKEY, 
            strm.ACCOUNTSERVICEKEY, 
            strm.ACCOUNTTRADEWASTEKEY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPEARANCE, 
            strm.AREA, 
            strm.BILLEDFLAG, 
            strm.CNTCTKEY, 
            strm.COMMENTKEY, 
            strm.DELETED, 
            strm.DOCKETNUMBER, 
            strm.EMPLOYEEID, 
            strm.FACILITY, 
            strm.GROSS, 
            strm.INDTTM, 
            strm.LATEFEEAMOUNT, 
            strm.LATEFEEFLAG, 
            strm.LINEITEMKEY, 
            strm.LOCATION, 
            strm.MANIFESTFLAG, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NET, 
            strm.NOMINATEDONEOFF, 
            strm.ODOR, 
            strm.ORIGIN, 
            strm.OUTDTTM, 
            strm.PURCHASENUMBER, 
            strm.TARE, 
            strm.TRAILERREGKEY, 
            strm.TWVEHICLEREGKEY, 
            strm.UNIT, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ACCOUNTTRADEWASTEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_ACCOUNTTRADEWASTE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ACCOUNTTRADEWASTEKEY=src.ACCOUNTTRADEWASTEKEY) OR (target.ACCOUNTTRADEWASTEKEY IS NULL AND src.ACCOUNTTRADEWASTEKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ACCOUNTSERVICEKEY=src.ACCOUNTSERVICEKEY, 
                    target.ACCOUNTTRADEWASTEKEY=src.ACCOUNTTRADEWASTEKEY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPEARANCE=src.APPEARANCE, 
                    target.AREA=src.AREA, 
                    target.BILLEDFLAG=src.BILLEDFLAG, 
                    target.CNTCTKEY=src.CNTCTKEY, 
                    target.COMMENTKEY=src.COMMENTKEY, 
                    target.DELETED=src.DELETED, 
                    target.DOCKETNUMBER=src.DOCKETNUMBER, 
                    target.EMPLOYEEID=src.EMPLOYEEID, 
                    target.FACILITY=src.FACILITY, 
                    target.GROSS=src.GROSS, 
                    target.INDTTM=src.INDTTM, 
                    target.LATEFEEAMOUNT=src.LATEFEEAMOUNT, 
                    target.LATEFEEFLAG=src.LATEFEEFLAG, 
                    target.LINEITEMKEY=src.LINEITEMKEY, 
                    target.LOCATION=src.LOCATION, 
                    target.MANIFESTFLAG=src.MANIFESTFLAG, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NET=src.NET, 
                    target.NOMINATEDONEOFF=src.NOMINATEDONEOFF, 
                    target.ODOR=src.ODOR, 
                    target.ORIGIN=src.ORIGIN, 
                    target.OUTDTTM=src.OUTDTTM, 
                    target.PURCHASENUMBER=src.PURCHASENUMBER, 
                    target.TARE=src.TARE, 
                    target.TRAILERREGKEY=src.TRAILERREGKEY, 
                    target.TWVEHICLEREGKEY=src.TWVEHICLEREGKEY, 
                    target.UNIT=src.UNIT, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTKEY, 
                    ACCOUNTSERVICEKEY, 
                    ACCOUNTTRADEWASTEKEY, 
                    ADDBY, 
                    ADDDTTM, 
                    APPEARANCE, 
                    AREA, 
                    BILLEDFLAG, 
                    CNTCTKEY, 
                    COMMENTKEY, 
                    DELETED, 
                    DOCKETNUMBER, 
                    EMPLOYEEID, 
                    FACILITY, 
                    GROSS, 
                    INDTTM, 
                    LATEFEEAMOUNT, 
                    LATEFEEFLAG, 
                    LINEITEMKEY, 
                    LOCATION, 
                    MANIFESTFLAG, 
                    MODBY, 
                    MODDTTM, 
                    NET, 
                    NOMINATEDONEOFF, 
                    ODOR, 
                    ORIGIN, 
                    OUTDTTM, 
                    PURCHASENUMBER, 
                    TARE, 
                    TRAILERREGKEY, 
                    TWVEHICLEREGKEY, 
                    UNIT, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTKEY, 
                    src.ACCOUNTSERVICEKEY, 
                    src.ACCOUNTTRADEWASTEKEY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPEARANCE, 
                    src.AREA, 
                    src.BILLEDFLAG, 
                    src.CNTCTKEY, 
                    src.COMMENTKEY, 
                    src.DELETED, 
                    src.DOCKETNUMBER, 
                    src.EMPLOYEEID, 
                    src.FACILITY, 
                    src.GROSS, 
                    src.INDTTM, 
                    src.LATEFEEAMOUNT, 
                    src.LATEFEEFLAG, 
                    src.LINEITEMKEY, 
                    src.LOCATION, 
                    src.MANIFESTFLAG, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NET, 
                    src.NOMINATEDONEOFF, 
                    src.ODOR, 
                    src.ORIGIN, 
                    src.OUTDTTM, 
                    src.PURCHASENUMBER, 
                    src.TARE, 
                    src.TRAILERREGKEY, 
                    src.TWVEHICLEREGKEY, 
                    src.UNIT, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_ACCOUNTTRADEWASTE as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTKEY, 
            strm.ACCOUNTSERVICEKEY, 
            strm.ACCOUNTTRADEWASTEKEY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPEARANCE, 
            strm.AREA, 
            strm.BILLEDFLAG, 
            strm.CNTCTKEY, 
            strm.COMMENTKEY, 
            strm.DELETED, 
            strm.DOCKETNUMBER, 
            strm.EMPLOYEEID, 
            strm.FACILITY, 
            strm.GROSS, 
            strm.INDTTM, 
            strm.LATEFEEAMOUNT, 
            strm.LATEFEEFLAG, 
            strm.LINEITEMKEY, 
            strm.LOCATION, 
            strm.MANIFESTFLAG, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NET, 
            strm.NOMINATEDONEOFF, 
            strm.ODOR, 
            strm.ORIGIN, 
            strm.OUTDTTM, 
            strm.PURCHASENUMBER, 
            strm.TARE, 
            strm.TRAILERREGKEY, 
            strm.TWVEHICLEREGKEY, 
            strm.UNIT, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ACCOUNTTRADEWASTEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_ACCOUNTTRADEWASTE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ACCOUNTTRADEWASTEKEY=src.ACCOUNTTRADEWASTEKEY) OR (target.ACCOUNTTRADEWASTEKEY IS NULL AND src.ACCOUNTTRADEWASTEKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ACCOUNTSERVICEKEY=src.ACCOUNTSERVICEKEY, 
                    target.ACCOUNTTRADEWASTEKEY=src.ACCOUNTTRADEWASTEKEY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPEARANCE=src.APPEARANCE, 
                    target.AREA=src.AREA, 
                    target.BILLEDFLAG=src.BILLEDFLAG, 
                    target.CNTCTKEY=src.CNTCTKEY, 
                    target.COMMENTKEY=src.COMMENTKEY, 
                    target.DELETED=src.DELETED, 
                    target.DOCKETNUMBER=src.DOCKETNUMBER, 
                    target.EMPLOYEEID=src.EMPLOYEEID, 
                    target.FACILITY=src.FACILITY, 
                    target.GROSS=src.GROSS, 
                    target.INDTTM=src.INDTTM, 
                    target.LATEFEEAMOUNT=src.LATEFEEAMOUNT, 
                    target.LATEFEEFLAG=src.LATEFEEFLAG, 
                    target.LINEITEMKEY=src.LINEITEMKEY, 
                    target.LOCATION=src.LOCATION, 
                    target.MANIFESTFLAG=src.MANIFESTFLAG, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NET=src.NET, 
                    target.NOMINATEDONEOFF=src.NOMINATEDONEOFF, 
                    target.ODOR=src.ODOR, 
                    target.ORIGIN=src.ORIGIN, 
                    target.OUTDTTM=src.OUTDTTM, 
                    target.PURCHASENUMBER=src.PURCHASENUMBER, 
                    target.TARE=src.TARE, 
                    target.TRAILERREGKEY=src.TRAILERREGKEY, 
                    target.TWVEHICLEREGKEY=src.TWVEHICLEREGKEY, 
                    target.UNIT=src.UNIT, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTKEY, ACCOUNTSERVICEKEY, ACCOUNTTRADEWASTEKEY, ADDBY, ADDDTTM, APPEARANCE, AREA, BILLEDFLAG, CNTCTKEY, COMMENTKEY, DELETED, DOCKETNUMBER, EMPLOYEEID, FACILITY, GROSS, INDTTM, LATEFEEAMOUNT, LATEFEEFLAG, LINEITEMKEY, LOCATION, MANIFESTFLAG, MODBY, MODDTTM, NET, NOMINATEDONEOFF, ODOR, ORIGIN, OUTDTTM, PURCHASENUMBER, TARE, TRAILERREGKEY, TWVEHICLEREGKEY, UNIT, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTKEY, 
                    src.ACCOUNTSERVICEKEY, 
                    src.ACCOUNTTRADEWASTEKEY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPEARANCE, 
                    src.AREA, 
                    src.BILLEDFLAG, 
                    src.CNTCTKEY, 
                    src.COMMENTKEY, 
                    src.DELETED, 
                    src.DOCKETNUMBER, 
                    src.EMPLOYEEID, 
                    src.FACILITY, 
                    src.GROSS, 
                    src.INDTTM, 
                    src.LATEFEEAMOUNT, 
                    src.LATEFEEFLAG, 
                    src.LINEITEMKEY, 
                    src.LOCATION, 
                    src.MANIFESTFLAG, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NET, 
                    src.NOMINATEDONEOFF, 
                    src.ODOR, 
                    src.ORIGIN, 
                    src.OUTDTTM, 
                    src.PURCHASENUMBER, 
                    src.TARE, 
                    src.TRAILERREGKEY, 
                    src.TWVEHICLEREGKEY, 
                    src.UNIT, 
                    src.VARIATION_ID,     
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
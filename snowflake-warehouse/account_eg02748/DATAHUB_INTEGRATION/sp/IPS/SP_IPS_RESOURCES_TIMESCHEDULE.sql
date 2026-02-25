create or replace procedure DATAHUB_INTEGRATION.SP_IPS_RESOURCES_TIMESCHEDULE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_RESOURCES_TIMESCHEDULE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_RESOURCES_TIMESCHEDULE as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.DATALAKE_DELETED, 
            strm.DAYFLAGS, 
            strm.DAYOFMONTH, 
            strm.DAYOFWEEK, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.MAXDAYSADV, 
            strm.MIGSRCKEY, 
            strm.MINDAYSADV, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MONTHOFYEAR, 
            strm.NEXTSCHEDDATE, 
            strm.REPEATHOURS, 
            strm.REPEATMINUTES, 
            strm.REPEATUNTILHOURS, 
            strm.REPEATUNTILMIN, 
            strm.REPEATUNTILTIME, 
            strm.SCHEDTYPE, 
            strm.SKIPDAYS, 
            strm.SKIPMONTHS, 
            strm.SKIPWEEKS, 
            strm.STARTDATE, 
            strm.TASKREPEAT, 
            strm.TIMESCHEDKEY, 
            strm.VARIATION_ID, 
            strm.WEEKFLAGS, 
            strm.WEEKOFMONTH, 
            strm.YEARLYDATE, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.TIMESCHEDKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_TIMESCHEDULE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.TIMESCHEDKEY=src.TIMESCHEDKEY) OR (target.TIMESCHEDKEY IS NULL AND src.TIMESCHEDKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DAYFLAGS=src.DAYFLAGS, 
                    target.DAYOFMONTH=src.DAYOFMONTH, 
                    target.DAYOFWEEK=src.DAYOFWEEK, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.MAXDAYSADV=src.MAXDAYSADV, 
                    target.MIGSRCKEY=src.MIGSRCKEY, 
                    target.MINDAYSADV=src.MINDAYSADV, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MONTHOFYEAR=src.MONTHOFYEAR, 
                    target.NEXTSCHEDDATE=src.NEXTSCHEDDATE, 
                    target.REPEATHOURS=src.REPEATHOURS, 
                    target.REPEATMINUTES=src.REPEATMINUTES, 
                    target.REPEATUNTILHOURS=src.REPEATUNTILHOURS, 
                    target.REPEATUNTILMIN=src.REPEATUNTILMIN, 
                    target.REPEATUNTILTIME=src.REPEATUNTILTIME, 
                    target.SCHEDTYPE=src.SCHEDTYPE, 
                    target.SKIPDAYS=src.SKIPDAYS, 
                    target.SKIPMONTHS=src.SKIPMONTHS, 
                    target.SKIPWEEKS=src.SKIPWEEKS, 
                    target.STARTDATE=src.STARTDATE, 
                    target.TASKREPEAT=src.TASKREPEAT, 
                    target.TIMESCHEDKEY=src.TIMESCHEDKEY, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WEEKFLAGS=src.WEEKFLAGS, 
                    target.WEEKOFMONTH=src.WEEKOFMONTH, 
                    target.YEARLYDATE=src.YEARLYDATE, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    DATALAKE_DELETED, 
                    DAYFLAGS, 
                    DAYOFMONTH, 
                    DAYOFWEEK, 
                    EFFDATE, 
                    EXPDATE, 
                    MAXDAYSADV, 
                    MIGSRCKEY, 
                    MINDAYSADV, 
                    MODBY, 
                    MODDTTM, 
                    MONTHOFYEAR, 
                    NEXTSCHEDDATE, 
                    REPEATHOURS, 
                    REPEATMINUTES, 
                    REPEATUNTILHOURS, 
                    REPEATUNTILMIN, 
                    REPEATUNTILTIME, 
                    SCHEDTYPE, 
                    SKIPDAYS, 
                    SKIPMONTHS, 
                    SKIPWEEKS, 
                    STARTDATE, 
                    TASKREPEAT, 
                    TIMESCHEDKEY, 
                    VARIATION_ID, 
                    WEEKFLAGS, 
                    WEEKOFMONTH, 
                    YEARLYDATE, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.DATALAKE_DELETED, 
                    src.DAYFLAGS, 
                    src.DAYOFMONTH, 
                    src.DAYOFWEEK, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.MAXDAYSADV, 
                    src.MIGSRCKEY, 
                    src.MINDAYSADV, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MONTHOFYEAR, 
                    src.NEXTSCHEDDATE, 
                    src.REPEATHOURS, 
                    src.REPEATMINUTES, 
                    src.REPEATUNTILHOURS, 
                    src.REPEATUNTILMIN, 
                    src.REPEATUNTILTIME, 
                    src.SCHEDTYPE, 
                    src.SKIPDAYS, 
                    src.SKIPMONTHS, 
                    src.SKIPWEEKS, 
                    src.STARTDATE, 
                    src.TASKREPEAT, 
                    src.TIMESCHEDKEY, 
                    src.VARIATION_ID, 
                    src.WEEKFLAGS, 
                    src.WEEKOFMONTH, 
                    src.YEARLYDATE,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_RESOURCES_TIMESCHEDULE as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.DATALAKE_DELETED, 
            strm.DAYFLAGS, 
            strm.DAYOFMONTH, 
            strm.DAYOFWEEK, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.MAXDAYSADV, 
            strm.MIGSRCKEY, 
            strm.MINDAYSADV, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MONTHOFYEAR, 
            strm.NEXTSCHEDDATE, 
            strm.REPEATHOURS, 
            strm.REPEATMINUTES, 
            strm.REPEATUNTILHOURS, 
            strm.REPEATUNTILMIN, 
            strm.REPEATUNTILTIME, 
            strm.SCHEDTYPE, 
            strm.SKIPDAYS, 
            strm.SKIPMONTHS, 
            strm.SKIPWEEKS, 
            strm.STARTDATE, 
            strm.TASKREPEAT, 
            strm.TIMESCHEDKEY, 
            strm.VARIATION_ID, 
            strm.WEEKFLAGS, 
            strm.WEEKOFMONTH, 
            strm.YEARLYDATE, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.TIMESCHEDKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_TIMESCHEDULE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.TIMESCHEDKEY=src.TIMESCHEDKEY) OR (target.TIMESCHEDKEY IS NULL AND src.TIMESCHEDKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DAYFLAGS=src.DAYFLAGS, 
                    target.DAYOFMONTH=src.DAYOFMONTH, 
                    target.DAYOFWEEK=src.DAYOFWEEK, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.MAXDAYSADV=src.MAXDAYSADV, 
                    target.MIGSRCKEY=src.MIGSRCKEY, 
                    target.MINDAYSADV=src.MINDAYSADV, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MONTHOFYEAR=src.MONTHOFYEAR, 
                    target.NEXTSCHEDDATE=src.NEXTSCHEDDATE, 
                    target.REPEATHOURS=src.REPEATHOURS, 
                    target.REPEATMINUTES=src.REPEATMINUTES, 
                    target.REPEATUNTILHOURS=src.REPEATUNTILHOURS, 
                    target.REPEATUNTILMIN=src.REPEATUNTILMIN, 
                    target.REPEATUNTILTIME=src.REPEATUNTILTIME, 
                    target.SCHEDTYPE=src.SCHEDTYPE, 
                    target.SKIPDAYS=src.SKIPDAYS, 
                    target.SKIPMONTHS=src.SKIPMONTHS, 
                    target.SKIPWEEKS=src.SKIPWEEKS, 
                    target.STARTDATE=src.STARTDATE, 
                    target.TASKREPEAT=src.TASKREPEAT, 
                    target.TIMESCHEDKEY=src.TIMESCHEDKEY, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WEEKFLAGS=src.WEEKFLAGS, 
                    target.WEEKOFMONTH=src.WEEKOFMONTH, 
                    target.YEARLYDATE=src.YEARLYDATE, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, DATALAKE_DELETED, DAYFLAGS, DAYOFMONTH, DAYOFWEEK, EFFDATE, EXPDATE, MAXDAYSADV, MIGSRCKEY, MINDAYSADV, MODBY, MODDTTM, MONTHOFYEAR, NEXTSCHEDDATE, REPEATHOURS, REPEATMINUTES, REPEATUNTILHOURS, REPEATUNTILMIN, REPEATUNTILTIME, SCHEDTYPE, SKIPDAYS, SKIPMONTHS, SKIPWEEKS, STARTDATE, TASKREPEAT, TIMESCHEDKEY, VARIATION_ID, WEEKFLAGS, WEEKOFMONTH, YEARLYDATE, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.DATALAKE_DELETED, 
                    src.DAYFLAGS, 
                    src.DAYOFMONTH, 
                    src.DAYOFWEEK, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.MAXDAYSADV, 
                    src.MIGSRCKEY, 
                    src.MINDAYSADV, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MONTHOFYEAR, 
                    src.NEXTSCHEDDATE, 
                    src.REPEATHOURS, 
                    src.REPEATMINUTES, 
                    src.REPEATUNTILHOURS, 
                    src.REPEATUNTILMIN, 
                    src.REPEATUNTILTIME, 
                    src.SCHEDTYPE, 
                    src.SKIPDAYS, 
                    src.SKIPMONTHS, 
                    src.SKIPWEEKS, 
                    src.STARTDATE, 
                    src.TASKREPEAT, 
                    src.TIMESCHEDKEY, 
                    src.VARIATION_ID, 
                    src.WEEKFLAGS, 
                    src.WEEKOFMONTH, 
                    src.YEARLYDATE,     
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
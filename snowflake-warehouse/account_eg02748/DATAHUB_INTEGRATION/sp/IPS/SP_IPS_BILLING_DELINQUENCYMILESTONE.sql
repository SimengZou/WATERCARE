create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_DELINQUENCYMILESTONE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_DELINQUENCYMILESTONE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_DELINQUENCYMILESTONE as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AUTODECREMENTBELOWAMT, 
            strm.AUTODECREMENTMILESTONEKEY, 
            strm.AUTODECREMENTMSFORMULA, 
            strm.AUTODECREMENTMSSPECIFIER, 
            strm.CODE, 
            strm.COLLECTIONSELIGIBLEFLAG, 
            strm.COLLECTIONSRESOLUTIONCODE, 
            strm.COLLRESCODEFORMULAKEY, 
            strm.COLLRESCODESPECIFIER, 
            strm.CREDITRATINGAPPLYTO, 
            strm.CREDITRATINGPOINTS, 
            strm.CREDITRATINGPOINTSFORMULA, 
            strm.CREDITRATINGSPECIFIER, 
            strm.DECREMENTMILESTONEFORMULA, 
            strm.DECREMENTMILESTONKEY, 
            strm.DECREMENTMILESTONSPECIFIER, 
            strm.DELETED, 
            strm.DELINQUENCYLEVEL, 
            strm.DISCONNECTMILESTONE, 
            strm.DISPLAYORDER, 
            strm.DUEAFTER, 
            strm.DUEAFTERDAYTYPE, 
            strm.DUEDATEENDONVALIDDAYTYPE, 
            strm.DUEDATESEARCHDIRECTION, 
            strm.DURATION, 
            strm.DURATIONDAYTYPE, 
            strm.DURATIONFORMULA, 
            strm.DURATIONSPECIFIER, 
            strm.LIENELIGIBLEFLAG, 
            strm.LIENRESCODEFORMULAKEY, 
            strm.LIENRESCODESPECIFIER, 
            strm.LIENRESOLUTIONCODE, 
            strm.MILESTONEKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NEXTMILESTONEFORMULA, 
            strm.NEXTMILESTONEKEY, 
            strm.NEXTMILESTONESPECIFIER, 
            strm.RESOLUTIONCODE, 
            strm.RESOLUTIONCODEFORMULA, 
            strm.RESOLUTIONCODESPECIFIER, 
            strm.RESOLVEBELOWAMT, 
            strm.RESOLVEFORMULA, 
            strm.RESOLVESPECIFIER, 
            strm.REVERSEPAYMILESTONEFORMULA, 
            strm.REVERSEPAYMILESTONEKEY, 
            strm.REVERSEPAYMSSPECIFIER, 
            strm.SCHEMEKEY, 
            strm.VARIATION_ID, 
            strm.WRITEOFFELIGIBLEFLAG, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MILESTONEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_DELINQUENCYMILESTONE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MILESTONEKEY=src.MILESTONEKEY) OR (target.MILESTONEKEY IS NULL AND src.MILESTONEKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AUTODECREMENTBELOWAMT=src.AUTODECREMENTBELOWAMT, 
                    target.AUTODECREMENTMILESTONEKEY=src.AUTODECREMENTMILESTONEKEY, 
                    target.AUTODECREMENTMSFORMULA=src.AUTODECREMENTMSFORMULA, 
                    target.AUTODECREMENTMSSPECIFIER=src.AUTODECREMENTMSSPECIFIER, 
                    target.CODE=src.CODE, 
                    target.COLLECTIONSELIGIBLEFLAG=src.COLLECTIONSELIGIBLEFLAG, 
                    target.COLLECTIONSRESOLUTIONCODE=src.COLLECTIONSRESOLUTIONCODE, 
                    target.COLLRESCODEFORMULAKEY=src.COLLRESCODEFORMULAKEY, 
                    target.COLLRESCODESPECIFIER=src.COLLRESCODESPECIFIER, 
                    target.CREDITRATINGAPPLYTO=src.CREDITRATINGAPPLYTO, 
                    target.CREDITRATINGPOINTS=src.CREDITRATINGPOINTS, 
                    target.CREDITRATINGPOINTSFORMULA=src.CREDITRATINGPOINTSFORMULA, 
                    target.CREDITRATINGSPECIFIER=src.CREDITRATINGSPECIFIER, 
                    target.DECREMENTMILESTONEFORMULA=src.DECREMENTMILESTONEFORMULA, 
                    target.DECREMENTMILESTONKEY=src.DECREMENTMILESTONKEY, 
                    target.DECREMENTMILESTONSPECIFIER=src.DECREMENTMILESTONSPECIFIER, 
                    target.DELETED=src.DELETED, 
                    target.DELINQUENCYLEVEL=src.DELINQUENCYLEVEL, 
                    target.DISCONNECTMILESTONE=src.DISCONNECTMILESTONE, 
                    target.DISPLAYORDER=src.DISPLAYORDER, 
                    target.DUEAFTER=src.DUEAFTER, 
                    target.DUEAFTERDAYTYPE=src.DUEAFTERDAYTYPE, 
                    target.DUEDATEENDONVALIDDAYTYPE=src.DUEDATEENDONVALIDDAYTYPE, 
                    target.DUEDATESEARCHDIRECTION=src.DUEDATESEARCHDIRECTION, 
                    target.DURATION=src.DURATION, 
                    target.DURATIONDAYTYPE=src.DURATIONDAYTYPE, 
                    target.DURATIONFORMULA=src.DURATIONFORMULA, 
                    target.DURATIONSPECIFIER=src.DURATIONSPECIFIER, 
                    target.LIENELIGIBLEFLAG=src.LIENELIGIBLEFLAG, 
                    target.LIENRESCODEFORMULAKEY=src.LIENRESCODEFORMULAKEY, 
                    target.LIENRESCODESPECIFIER=src.LIENRESCODESPECIFIER, 
                    target.LIENRESOLUTIONCODE=src.LIENRESOLUTIONCODE, 
                    target.MILESTONEKEY=src.MILESTONEKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NEXTMILESTONEFORMULA=src.NEXTMILESTONEFORMULA, 
                    target.NEXTMILESTONEKEY=src.NEXTMILESTONEKEY, 
                    target.NEXTMILESTONESPECIFIER=src.NEXTMILESTONESPECIFIER, 
                    target.RESOLUTIONCODE=src.RESOLUTIONCODE, 
                    target.RESOLUTIONCODEFORMULA=src.RESOLUTIONCODEFORMULA, 
                    target.RESOLUTIONCODESPECIFIER=src.RESOLUTIONCODESPECIFIER, 
                    target.RESOLVEBELOWAMT=src.RESOLVEBELOWAMT, 
                    target.RESOLVEFORMULA=src.RESOLVEFORMULA, 
                    target.RESOLVESPECIFIER=src.RESOLVESPECIFIER, 
                    target.REVERSEPAYMILESTONEFORMULA=src.REVERSEPAYMILESTONEFORMULA, 
                    target.REVERSEPAYMILESTONEKEY=src.REVERSEPAYMILESTONEKEY, 
                    target.REVERSEPAYMSSPECIFIER=src.REVERSEPAYMSSPECIFIER, 
                    target.SCHEMEKEY=src.SCHEMEKEY, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WRITEOFFELIGIBLEFLAG=src.WRITEOFFELIGIBLEFLAG, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    AUTODECREMENTBELOWAMT, 
                    AUTODECREMENTMILESTONEKEY, 
                    AUTODECREMENTMSFORMULA, 
                    AUTODECREMENTMSSPECIFIER, 
                    CODE, 
                    COLLECTIONSELIGIBLEFLAG, 
                    COLLECTIONSRESOLUTIONCODE, 
                    COLLRESCODEFORMULAKEY, 
                    COLLRESCODESPECIFIER, 
                    CREDITRATINGAPPLYTO, 
                    CREDITRATINGPOINTS, 
                    CREDITRATINGPOINTSFORMULA, 
                    CREDITRATINGSPECIFIER, 
                    DECREMENTMILESTONEFORMULA, 
                    DECREMENTMILESTONKEY, 
                    DECREMENTMILESTONSPECIFIER, 
                    DELETED, 
                    DELINQUENCYLEVEL, 
                    DISCONNECTMILESTONE, 
                    DISPLAYORDER, 
                    DUEAFTER, 
                    DUEAFTERDAYTYPE, 
                    DUEDATEENDONVALIDDAYTYPE, 
                    DUEDATESEARCHDIRECTION, 
                    DURATION, 
                    DURATIONDAYTYPE, 
                    DURATIONFORMULA, 
                    DURATIONSPECIFIER, 
                    LIENELIGIBLEFLAG, 
                    LIENRESCODEFORMULAKEY, 
                    LIENRESCODESPECIFIER, 
                    LIENRESOLUTIONCODE, 
                    MILESTONEKEY, 
                    MODBY, 
                    MODDTTM, 
                    NEXTMILESTONEFORMULA, 
                    NEXTMILESTONEKEY, 
                    NEXTMILESTONESPECIFIER, 
                    RESOLUTIONCODE, 
                    RESOLUTIONCODEFORMULA, 
                    RESOLUTIONCODESPECIFIER, 
                    RESOLVEBELOWAMT, 
                    RESOLVEFORMULA, 
                    RESOLVESPECIFIER, 
                    REVERSEPAYMILESTONEFORMULA, 
                    REVERSEPAYMILESTONEKEY, 
                    REVERSEPAYMSSPECIFIER, 
                    SCHEMEKEY, 
                    VARIATION_ID, 
                    WRITEOFFELIGIBLEFLAG, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AUTODECREMENTBELOWAMT, 
                    src.AUTODECREMENTMILESTONEKEY, 
                    src.AUTODECREMENTMSFORMULA, 
                    src.AUTODECREMENTMSSPECIFIER, 
                    src.CODE, 
                    src.COLLECTIONSELIGIBLEFLAG, 
                    src.COLLECTIONSRESOLUTIONCODE, 
                    src.COLLRESCODEFORMULAKEY, 
                    src.COLLRESCODESPECIFIER, 
                    src.CREDITRATINGAPPLYTO, 
                    src.CREDITRATINGPOINTS, 
                    src.CREDITRATINGPOINTSFORMULA, 
                    src.CREDITRATINGSPECIFIER, 
                    src.DECREMENTMILESTONEFORMULA, 
                    src.DECREMENTMILESTONKEY, 
                    src.DECREMENTMILESTONSPECIFIER, 
                    src.DELETED, 
                    src.DELINQUENCYLEVEL, 
                    src.DISCONNECTMILESTONE, 
                    src.DISPLAYORDER, 
                    src.DUEAFTER, 
                    src.DUEAFTERDAYTYPE, 
                    src.DUEDATEENDONVALIDDAYTYPE, 
                    src.DUEDATESEARCHDIRECTION, 
                    src.DURATION, 
                    src.DURATIONDAYTYPE, 
                    src.DURATIONFORMULA, 
                    src.DURATIONSPECIFIER, 
                    src.LIENELIGIBLEFLAG, 
                    src.LIENRESCODEFORMULAKEY, 
                    src.LIENRESCODESPECIFIER, 
                    src.LIENRESOLUTIONCODE, 
                    src.MILESTONEKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NEXTMILESTONEFORMULA, 
                    src.NEXTMILESTONEKEY, 
                    src.NEXTMILESTONESPECIFIER, 
                    src.RESOLUTIONCODE, 
                    src.RESOLUTIONCODEFORMULA, 
                    src.RESOLUTIONCODESPECIFIER, 
                    src.RESOLVEBELOWAMT, 
                    src.RESOLVEFORMULA, 
                    src.RESOLVESPECIFIER, 
                    src.REVERSEPAYMILESTONEFORMULA, 
                    src.REVERSEPAYMILESTONEKEY, 
                    src.REVERSEPAYMSSPECIFIER, 
                    src.SCHEMEKEY, 
                    src.VARIATION_ID, 
                    src.WRITEOFFELIGIBLEFLAG,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_DELINQUENCYMILESTONE as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AUTODECREMENTBELOWAMT, 
            strm.AUTODECREMENTMILESTONEKEY, 
            strm.AUTODECREMENTMSFORMULA, 
            strm.AUTODECREMENTMSSPECIFIER, 
            strm.CODE, 
            strm.COLLECTIONSELIGIBLEFLAG, 
            strm.COLLECTIONSRESOLUTIONCODE, 
            strm.COLLRESCODEFORMULAKEY, 
            strm.COLLRESCODESPECIFIER, 
            strm.CREDITRATINGAPPLYTO, 
            strm.CREDITRATINGPOINTS, 
            strm.CREDITRATINGPOINTSFORMULA, 
            strm.CREDITRATINGSPECIFIER, 
            strm.DECREMENTMILESTONEFORMULA, 
            strm.DECREMENTMILESTONKEY, 
            strm.DECREMENTMILESTONSPECIFIER, 
            strm.DELETED, 
            strm.DELINQUENCYLEVEL, 
            strm.DISCONNECTMILESTONE, 
            strm.DISPLAYORDER, 
            strm.DUEAFTER, 
            strm.DUEAFTERDAYTYPE, 
            strm.DUEDATEENDONVALIDDAYTYPE, 
            strm.DUEDATESEARCHDIRECTION, 
            strm.DURATION, 
            strm.DURATIONDAYTYPE, 
            strm.DURATIONFORMULA, 
            strm.DURATIONSPECIFIER, 
            strm.LIENELIGIBLEFLAG, 
            strm.LIENRESCODEFORMULAKEY, 
            strm.LIENRESCODESPECIFIER, 
            strm.LIENRESOLUTIONCODE, 
            strm.MILESTONEKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NEXTMILESTONEFORMULA, 
            strm.NEXTMILESTONEKEY, 
            strm.NEXTMILESTONESPECIFIER, 
            strm.RESOLUTIONCODE, 
            strm.RESOLUTIONCODEFORMULA, 
            strm.RESOLUTIONCODESPECIFIER, 
            strm.RESOLVEBELOWAMT, 
            strm.RESOLVEFORMULA, 
            strm.RESOLVESPECIFIER, 
            strm.REVERSEPAYMILESTONEFORMULA, 
            strm.REVERSEPAYMILESTONEKEY, 
            strm.REVERSEPAYMSSPECIFIER, 
            strm.SCHEMEKEY, 
            strm.VARIATION_ID, 
            strm.WRITEOFFELIGIBLEFLAG, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MILESTONEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_DELINQUENCYMILESTONE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MILESTONEKEY=src.MILESTONEKEY) OR (target.MILESTONEKEY IS NULL AND src.MILESTONEKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AUTODECREMENTBELOWAMT=src.AUTODECREMENTBELOWAMT, 
                    target.AUTODECREMENTMILESTONEKEY=src.AUTODECREMENTMILESTONEKEY, 
                    target.AUTODECREMENTMSFORMULA=src.AUTODECREMENTMSFORMULA, 
                    target.AUTODECREMENTMSSPECIFIER=src.AUTODECREMENTMSSPECIFIER, 
                    target.CODE=src.CODE, 
                    target.COLLECTIONSELIGIBLEFLAG=src.COLLECTIONSELIGIBLEFLAG, 
                    target.COLLECTIONSRESOLUTIONCODE=src.COLLECTIONSRESOLUTIONCODE, 
                    target.COLLRESCODEFORMULAKEY=src.COLLRESCODEFORMULAKEY, 
                    target.COLLRESCODESPECIFIER=src.COLLRESCODESPECIFIER, 
                    target.CREDITRATINGAPPLYTO=src.CREDITRATINGAPPLYTO, 
                    target.CREDITRATINGPOINTS=src.CREDITRATINGPOINTS, 
                    target.CREDITRATINGPOINTSFORMULA=src.CREDITRATINGPOINTSFORMULA, 
                    target.CREDITRATINGSPECIFIER=src.CREDITRATINGSPECIFIER, 
                    target.DECREMENTMILESTONEFORMULA=src.DECREMENTMILESTONEFORMULA, 
                    target.DECREMENTMILESTONKEY=src.DECREMENTMILESTONKEY, 
                    target.DECREMENTMILESTONSPECIFIER=src.DECREMENTMILESTONSPECIFIER, 
                    target.DELETED=src.DELETED, 
                    target.DELINQUENCYLEVEL=src.DELINQUENCYLEVEL, 
                    target.DISCONNECTMILESTONE=src.DISCONNECTMILESTONE, 
                    target.DISPLAYORDER=src.DISPLAYORDER, 
                    target.DUEAFTER=src.DUEAFTER, 
                    target.DUEAFTERDAYTYPE=src.DUEAFTERDAYTYPE, 
                    target.DUEDATEENDONVALIDDAYTYPE=src.DUEDATEENDONVALIDDAYTYPE, 
                    target.DUEDATESEARCHDIRECTION=src.DUEDATESEARCHDIRECTION, 
                    target.DURATION=src.DURATION, 
                    target.DURATIONDAYTYPE=src.DURATIONDAYTYPE, 
                    target.DURATIONFORMULA=src.DURATIONFORMULA, 
                    target.DURATIONSPECIFIER=src.DURATIONSPECIFIER, 
                    target.LIENELIGIBLEFLAG=src.LIENELIGIBLEFLAG, 
                    target.LIENRESCODEFORMULAKEY=src.LIENRESCODEFORMULAKEY, 
                    target.LIENRESCODESPECIFIER=src.LIENRESCODESPECIFIER, 
                    target.LIENRESOLUTIONCODE=src.LIENRESOLUTIONCODE, 
                    target.MILESTONEKEY=src.MILESTONEKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NEXTMILESTONEFORMULA=src.NEXTMILESTONEFORMULA, 
                    target.NEXTMILESTONEKEY=src.NEXTMILESTONEKEY, 
                    target.NEXTMILESTONESPECIFIER=src.NEXTMILESTONESPECIFIER, 
                    target.RESOLUTIONCODE=src.RESOLUTIONCODE, 
                    target.RESOLUTIONCODEFORMULA=src.RESOLUTIONCODEFORMULA, 
                    target.RESOLUTIONCODESPECIFIER=src.RESOLUTIONCODESPECIFIER, 
                    target.RESOLVEBELOWAMT=src.RESOLVEBELOWAMT, 
                    target.RESOLVEFORMULA=src.RESOLVEFORMULA, 
                    target.RESOLVESPECIFIER=src.RESOLVESPECIFIER, 
                    target.REVERSEPAYMILESTONEFORMULA=src.REVERSEPAYMILESTONEFORMULA, 
                    target.REVERSEPAYMILESTONEKEY=src.REVERSEPAYMILESTONEKEY, 
                    target.REVERSEPAYMSSPECIFIER=src.REVERSEPAYMSSPECIFIER, 
                    target.SCHEMEKEY=src.SCHEMEKEY, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WRITEOFFELIGIBLEFLAG=src.WRITEOFFELIGIBLEFLAG, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, AUTODECREMENTBELOWAMT, AUTODECREMENTMILESTONEKEY, AUTODECREMENTMSFORMULA, AUTODECREMENTMSSPECIFIER, CODE, COLLECTIONSELIGIBLEFLAG, COLLECTIONSRESOLUTIONCODE, COLLRESCODEFORMULAKEY, COLLRESCODESPECIFIER, CREDITRATINGAPPLYTO, CREDITRATINGPOINTS, CREDITRATINGPOINTSFORMULA, CREDITRATINGSPECIFIER, DECREMENTMILESTONEFORMULA, DECREMENTMILESTONKEY, DECREMENTMILESTONSPECIFIER, DELETED, DELINQUENCYLEVEL, DISCONNECTMILESTONE, DISPLAYORDER, DUEAFTER, DUEAFTERDAYTYPE, DUEDATEENDONVALIDDAYTYPE, DUEDATESEARCHDIRECTION, DURATION, DURATIONDAYTYPE, DURATIONFORMULA, DURATIONSPECIFIER, LIENELIGIBLEFLAG, LIENRESCODEFORMULAKEY, LIENRESCODESPECIFIER, LIENRESOLUTIONCODE, MILESTONEKEY, MODBY, MODDTTM, NEXTMILESTONEFORMULA, NEXTMILESTONEKEY, NEXTMILESTONESPECIFIER, RESOLUTIONCODE, RESOLUTIONCODEFORMULA, RESOLUTIONCODESPECIFIER, RESOLVEBELOWAMT, RESOLVEFORMULA, RESOLVESPECIFIER, REVERSEPAYMILESTONEFORMULA, REVERSEPAYMILESTONEKEY, REVERSEPAYMSSPECIFIER, SCHEMEKEY, VARIATION_ID, WRITEOFFELIGIBLEFLAG, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AUTODECREMENTBELOWAMT, 
                    src.AUTODECREMENTMILESTONEKEY, 
                    src.AUTODECREMENTMSFORMULA, 
                    src.AUTODECREMENTMSSPECIFIER, 
                    src.CODE, 
                    src.COLLECTIONSELIGIBLEFLAG, 
                    src.COLLECTIONSRESOLUTIONCODE, 
                    src.COLLRESCODEFORMULAKEY, 
                    src.COLLRESCODESPECIFIER, 
                    src.CREDITRATINGAPPLYTO, 
                    src.CREDITRATINGPOINTS, 
                    src.CREDITRATINGPOINTSFORMULA, 
                    src.CREDITRATINGSPECIFIER, 
                    src.DECREMENTMILESTONEFORMULA, 
                    src.DECREMENTMILESTONKEY, 
                    src.DECREMENTMILESTONSPECIFIER, 
                    src.DELETED, 
                    src.DELINQUENCYLEVEL, 
                    src.DISCONNECTMILESTONE, 
                    src.DISPLAYORDER, 
                    src.DUEAFTER, 
                    src.DUEAFTERDAYTYPE, 
                    src.DUEDATEENDONVALIDDAYTYPE, 
                    src.DUEDATESEARCHDIRECTION, 
                    src.DURATION, 
                    src.DURATIONDAYTYPE, 
                    src.DURATIONFORMULA, 
                    src.DURATIONSPECIFIER, 
                    src.LIENELIGIBLEFLAG, 
                    src.LIENRESCODEFORMULAKEY, 
                    src.LIENRESCODESPECIFIER, 
                    src.LIENRESOLUTIONCODE, 
                    src.MILESTONEKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NEXTMILESTONEFORMULA, 
                    src.NEXTMILESTONEKEY, 
                    src.NEXTMILESTONESPECIFIER, 
                    src.RESOLUTIONCODE, 
                    src.RESOLUTIONCODEFORMULA, 
                    src.RESOLUTIONCODESPECIFIER, 
                    src.RESOLVEBELOWAMT, 
                    src.RESOLVEFORMULA, 
                    src.RESOLVESPECIFIER, 
                    src.REVERSEPAYMILESTONEFORMULA, 
                    src.REVERSEPAYMILESTONEKEY, 
                    src.REVERSEPAYMSSPECIFIER, 
                    src.SCHEMEKEY, 
                    src.VARIATION_ID, 
                    src.WRITEOFFELIGIBLEFLAG,     
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
create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_DIRECTDEBITSETUP()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_DIRECTDEBITSETUP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_DIRECTDEBITSETUP as target using (SELECT * FROM (SELECT 
            strm.ACHFORMATFILE, 
            strm.ACHOUTPUTDIRECTORY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.COMBINEAMOUNT, 
            strm.DATALAKE_DELETED, 
            strm.DIRECTDEBITAMTCRITERIA, 
            strm.DIRECTDEBITCYCLE1DAY, 
            strm.DIRECTDEBITCYCLE2DAY, 
            strm.DIRECTDEBITCYCLE3DAY, 
            strm.DIRECTDEBITCYCLE4DAY, 
            strm.DIRECTDEBITCYCLENUMBER, 
            strm.DIRECTDEBITSETUPKEY, 
            strm.ENABLEOTHERFLAG, 
            strm.FREQUENCYDAYS, 
            strm.FREQUENCYMONTHS, 
            strm.FREQUENCYWEEKS, 
            strm.INCLUDEUNBILLED, 
            strm.MAXIMUMAMOUNT, 
            strm.MAXIMUMSUSPENSIONDAY, 
            strm.MAXIMUMSUSPENSIONNUMBER, 
            strm.MINIMUMAMOUNT, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PRENOTE, 
            strm.PRENOTEEXTRACTDATEDAYS, 
            strm.PRENOTENUMBEROFDAYS, 
            strm.REFBALTHRUEXTRDATE, 
            strm.REFRESHEXTRACTAMOUNT, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DIRECTDEBITSETUPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_DIRECTDEBITSETUP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DIRECTDEBITSETUPKEY=src.DIRECTDEBITSETUPKEY) OR (target.DIRECTDEBITSETUPKEY IS NULL AND src.DIRECTDEBITSETUPKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACHFORMATFILE=src.ACHFORMATFILE, 
                    target.ACHOUTPUTDIRECTORY=src.ACHOUTPUTDIRECTORY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.COMBINEAMOUNT=src.COMBINEAMOUNT, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DIRECTDEBITAMTCRITERIA=src.DIRECTDEBITAMTCRITERIA, 
                    target.DIRECTDEBITCYCLE1DAY=src.DIRECTDEBITCYCLE1DAY, 
                    target.DIRECTDEBITCYCLE2DAY=src.DIRECTDEBITCYCLE2DAY, 
                    target.DIRECTDEBITCYCLE3DAY=src.DIRECTDEBITCYCLE3DAY, 
                    target.DIRECTDEBITCYCLE4DAY=src.DIRECTDEBITCYCLE4DAY, 
                    target.DIRECTDEBITCYCLENUMBER=src.DIRECTDEBITCYCLENUMBER, 
                    target.DIRECTDEBITSETUPKEY=src.DIRECTDEBITSETUPKEY, 
                    target.ENABLEOTHERFLAG=src.ENABLEOTHERFLAG, 
                    target.FREQUENCYDAYS=src.FREQUENCYDAYS, 
                    target.FREQUENCYMONTHS=src.FREQUENCYMONTHS, 
                    target.FREQUENCYWEEKS=src.FREQUENCYWEEKS, 
                    target.INCLUDEUNBILLED=src.INCLUDEUNBILLED, 
                    target.MAXIMUMAMOUNT=src.MAXIMUMAMOUNT, 
                    target.MAXIMUMSUSPENSIONDAY=src.MAXIMUMSUSPENSIONDAY, 
                    target.MAXIMUMSUSPENSIONNUMBER=src.MAXIMUMSUSPENSIONNUMBER, 
                    target.MINIMUMAMOUNT=src.MINIMUMAMOUNT, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PRENOTE=src.PRENOTE, 
                    target.PRENOTEEXTRACTDATEDAYS=src.PRENOTEEXTRACTDATEDAYS, 
                    target.PRENOTENUMBEROFDAYS=src.PRENOTENUMBEROFDAYS, 
                    target.REFBALTHRUEXTRDATE=src.REFBALTHRUEXTRDATE, 
                    target.REFRESHEXTRACTAMOUNT=src.REFRESHEXTRACTAMOUNT, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACHFORMATFILE, 
                    ACHOUTPUTDIRECTORY, 
                    ADDBY, 
                    ADDDTTM, 
                    COMBINEAMOUNT, 
                    DATALAKE_DELETED, 
                    DIRECTDEBITAMTCRITERIA, 
                    DIRECTDEBITCYCLE1DAY, 
                    DIRECTDEBITCYCLE2DAY, 
                    DIRECTDEBITCYCLE3DAY, 
                    DIRECTDEBITCYCLE4DAY, 
                    DIRECTDEBITCYCLENUMBER, 
                    DIRECTDEBITSETUPKEY, 
                    ENABLEOTHERFLAG, 
                    FREQUENCYDAYS, 
                    FREQUENCYMONTHS, 
                    FREQUENCYWEEKS, 
                    INCLUDEUNBILLED, 
                    MAXIMUMAMOUNT, 
                    MAXIMUMSUSPENSIONDAY, 
                    MAXIMUMSUSPENSIONNUMBER, 
                    MINIMUMAMOUNT, 
                    MODBY, 
                    MODDTTM, 
                    PRENOTE, 
                    PRENOTEEXTRACTDATEDAYS, 
                    PRENOTENUMBEROFDAYS, 
                    REFBALTHRUEXTRDATE, 
                    REFRESHEXTRACTAMOUNT, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACHFORMATFILE, 
                    src.ACHOUTPUTDIRECTORY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.COMBINEAMOUNT, 
                    src.DATALAKE_DELETED, 
                    src.DIRECTDEBITAMTCRITERIA, 
                    src.DIRECTDEBITCYCLE1DAY, 
                    src.DIRECTDEBITCYCLE2DAY, 
                    src.DIRECTDEBITCYCLE3DAY, 
                    src.DIRECTDEBITCYCLE4DAY, 
                    src.DIRECTDEBITCYCLENUMBER, 
                    src.DIRECTDEBITSETUPKEY, 
                    src.ENABLEOTHERFLAG, 
                    src.FREQUENCYDAYS, 
                    src.FREQUENCYMONTHS, 
                    src.FREQUENCYWEEKS, 
                    src.INCLUDEUNBILLED, 
                    src.MAXIMUMAMOUNT, 
                    src.MAXIMUMSUSPENSIONDAY, 
                    src.MAXIMUMSUSPENSIONNUMBER, 
                    src.MINIMUMAMOUNT, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PRENOTE, 
                    src.PRENOTEEXTRACTDATEDAYS, 
                    src.PRENOTENUMBEROFDAYS, 
                    src.REFBALTHRUEXTRDATE, 
                    src.REFRESHEXTRACTAMOUNT, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_DIRECTDEBITSETUP as target using (
                SELECT * FROM (SELECT 
            strm.ACHFORMATFILE, 
            strm.ACHOUTPUTDIRECTORY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.COMBINEAMOUNT, 
            strm.DATALAKE_DELETED, 
            strm.DIRECTDEBITAMTCRITERIA, 
            strm.DIRECTDEBITCYCLE1DAY, 
            strm.DIRECTDEBITCYCLE2DAY, 
            strm.DIRECTDEBITCYCLE3DAY, 
            strm.DIRECTDEBITCYCLE4DAY, 
            strm.DIRECTDEBITCYCLENUMBER, 
            strm.DIRECTDEBITSETUPKEY, 
            strm.ENABLEOTHERFLAG, 
            strm.FREQUENCYDAYS, 
            strm.FREQUENCYMONTHS, 
            strm.FREQUENCYWEEKS, 
            strm.INCLUDEUNBILLED, 
            strm.MAXIMUMAMOUNT, 
            strm.MAXIMUMSUSPENSIONDAY, 
            strm.MAXIMUMSUSPENSIONNUMBER, 
            strm.MINIMUMAMOUNT, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PRENOTE, 
            strm.PRENOTEEXTRACTDATEDAYS, 
            strm.PRENOTENUMBEROFDAYS, 
            strm.REFBALTHRUEXTRDATE, 
            strm.REFRESHEXTRACTAMOUNT, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DIRECTDEBITSETUPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_DIRECTDEBITSETUP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DIRECTDEBITSETUPKEY=src.DIRECTDEBITSETUPKEY) OR (target.DIRECTDEBITSETUPKEY IS NULL AND src.DIRECTDEBITSETUPKEY IS NULL)) 
                when matched then update set
                    target.ACHFORMATFILE=src.ACHFORMATFILE, 
                    target.ACHOUTPUTDIRECTORY=src.ACHOUTPUTDIRECTORY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.COMBINEAMOUNT=src.COMBINEAMOUNT, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DIRECTDEBITAMTCRITERIA=src.DIRECTDEBITAMTCRITERIA, 
                    target.DIRECTDEBITCYCLE1DAY=src.DIRECTDEBITCYCLE1DAY, 
                    target.DIRECTDEBITCYCLE2DAY=src.DIRECTDEBITCYCLE2DAY, 
                    target.DIRECTDEBITCYCLE3DAY=src.DIRECTDEBITCYCLE3DAY, 
                    target.DIRECTDEBITCYCLE4DAY=src.DIRECTDEBITCYCLE4DAY, 
                    target.DIRECTDEBITCYCLENUMBER=src.DIRECTDEBITCYCLENUMBER, 
                    target.DIRECTDEBITSETUPKEY=src.DIRECTDEBITSETUPKEY, 
                    target.ENABLEOTHERFLAG=src.ENABLEOTHERFLAG, 
                    target.FREQUENCYDAYS=src.FREQUENCYDAYS, 
                    target.FREQUENCYMONTHS=src.FREQUENCYMONTHS, 
                    target.FREQUENCYWEEKS=src.FREQUENCYWEEKS, 
                    target.INCLUDEUNBILLED=src.INCLUDEUNBILLED, 
                    target.MAXIMUMAMOUNT=src.MAXIMUMAMOUNT, 
                    target.MAXIMUMSUSPENSIONDAY=src.MAXIMUMSUSPENSIONDAY, 
                    target.MAXIMUMSUSPENSIONNUMBER=src.MAXIMUMSUSPENSIONNUMBER, 
                    target.MINIMUMAMOUNT=src.MINIMUMAMOUNT, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PRENOTE=src.PRENOTE, 
                    target.PRENOTEEXTRACTDATEDAYS=src.PRENOTEEXTRACTDATEDAYS, 
                    target.PRENOTENUMBEROFDAYS=src.PRENOTENUMBEROFDAYS, 
                    target.REFBALTHRUEXTRDATE=src.REFBALTHRUEXTRDATE, 
                    target.REFRESHEXTRACTAMOUNT=src.REFRESHEXTRACTAMOUNT, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACHFORMATFILE, ACHOUTPUTDIRECTORY, ADDBY, ADDDTTM, COMBINEAMOUNT, DATALAKE_DELETED, DIRECTDEBITAMTCRITERIA, DIRECTDEBITCYCLE1DAY, DIRECTDEBITCYCLE2DAY, DIRECTDEBITCYCLE3DAY, DIRECTDEBITCYCLE4DAY, DIRECTDEBITCYCLENUMBER, DIRECTDEBITSETUPKEY, ENABLEOTHERFLAG, FREQUENCYDAYS, FREQUENCYMONTHS, FREQUENCYWEEKS, INCLUDEUNBILLED, MAXIMUMAMOUNT, MAXIMUMSUSPENSIONDAY, MAXIMUMSUSPENSIONNUMBER, MINIMUMAMOUNT, MODBY, MODDTTM, PRENOTE, PRENOTEEXTRACTDATEDAYS, PRENOTENUMBEROFDAYS, REFBALTHRUEXTRDATE, REFRESHEXTRACTAMOUNT, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACHFORMATFILE, 
                    src.ACHOUTPUTDIRECTORY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.COMBINEAMOUNT, 
                    src.DATALAKE_DELETED, 
                    src.DIRECTDEBITAMTCRITERIA, 
                    src.DIRECTDEBITCYCLE1DAY, 
                    src.DIRECTDEBITCYCLE2DAY, 
                    src.DIRECTDEBITCYCLE3DAY, 
                    src.DIRECTDEBITCYCLE4DAY, 
                    src.DIRECTDEBITCYCLENUMBER, 
                    src.DIRECTDEBITSETUPKEY, 
                    src.ENABLEOTHERFLAG, 
                    src.FREQUENCYDAYS, 
                    src.FREQUENCYMONTHS, 
                    src.FREQUENCYWEEKS, 
                    src.INCLUDEUNBILLED, 
                    src.MAXIMUMAMOUNT, 
                    src.MAXIMUMSUSPENSIONDAY, 
                    src.MAXIMUMSUSPENSIONNUMBER, 
                    src.MINIMUMAMOUNT, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PRENOTE, 
                    src.PRENOTEEXTRACTDATEDAYS, 
                    src.PRENOTENUMBEROFDAYS, 
                    src.REFBALTHRUEXTRDATE, 
                    src.REFRESHEXTRACTAMOUNT, 
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
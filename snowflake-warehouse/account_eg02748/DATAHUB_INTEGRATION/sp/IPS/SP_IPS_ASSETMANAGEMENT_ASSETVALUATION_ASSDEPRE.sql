create or replace procedure DATAHUB_INTEGRATION.SP_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSDEPRE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSDEPRE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSDEPRE as target using (SELECT * FROM (SELECT 
            strm.ACCDEPREC, 
            strm.ACCPERIOD, 
            strm.ACQDATE, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ASSVALDATE, 
            strm.ASSVALKEY, 
            strm.ASSVALRUNKEY, 
            strm.BACKDATEADJFLAG, 
            strm.BGTKEY, 
            strm.BGTNO, 
            strm.CAPTLEXP, 
            strm.DATALAKE_DELETED, 
            strm.DEPRECAMT, 
            strm.DEPRECDATE, 
            strm.DEPRECKEY, 
            strm.DEPRECPERD, 
            strm.DEPRECRATE, 
            strm.DEPRECTYPE, 
            strm.DEPRECYTD, 
            strm.DISPSLFLAG, 
            strm.EFFECTIVEDTTM, 
            strm.EXPLIFE, 
            strm.JOURNALDATE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PERIODADJDEPREC, 
            strm.RECLDEPKEY, 
            strm.RECLNO, 
            strm.RESIDLIFE, 
            strm.RESIDVAL, 
            strm.REVALFINALCOST, 
            strm.REVALFLAG, 
            strm.TOBESOLDFLAG, 
            strm.VARIATION_ID, 
            strm.WRITEDNVAL, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DEPRECKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSDEPRE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DEPRECKEY=src.DEPRECKEY) OR (target.DEPRECKEY IS NULL AND src.DEPRECKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCDEPREC=src.ACCDEPREC, 
                    target.ACCPERIOD=src.ACCPERIOD, 
                    target.ACQDATE=src.ACQDATE, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ASSVALDATE=src.ASSVALDATE, 
                    target.ASSVALKEY=src.ASSVALKEY, 
                    target.ASSVALRUNKEY=src.ASSVALRUNKEY, 
                    target.BACKDATEADJFLAG=src.BACKDATEADJFLAG, 
                    target.BGTKEY=src.BGTKEY, 
                    target.BGTNO=src.BGTNO, 
                    target.CAPTLEXP=src.CAPTLEXP, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DEPRECAMT=src.DEPRECAMT, 
                    target.DEPRECDATE=src.DEPRECDATE, 
                    target.DEPRECKEY=src.DEPRECKEY, 
                    target.DEPRECPERD=src.DEPRECPERD, 
                    target.DEPRECRATE=src.DEPRECRATE, 
                    target.DEPRECTYPE=src.DEPRECTYPE, 
                    target.DEPRECYTD=src.DEPRECYTD, 
                    target.DISPSLFLAG=src.DISPSLFLAG, 
                    target.EFFECTIVEDTTM=src.EFFECTIVEDTTM, 
                    target.EXPLIFE=src.EXPLIFE, 
                    target.JOURNALDATE=src.JOURNALDATE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PERIODADJDEPREC=src.PERIODADJDEPREC, 
                    target.RECLDEPKEY=src.RECLDEPKEY, 
                    target.RECLNO=src.RECLNO, 
                    target.RESIDLIFE=src.RESIDLIFE, 
                    target.RESIDVAL=src.RESIDVAL, 
                    target.REVALFINALCOST=src.REVALFINALCOST, 
                    target.REVALFLAG=src.REVALFLAG, 
                    target.TOBESOLDFLAG=src.TOBESOLDFLAG, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WRITEDNVAL=src.WRITEDNVAL, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCDEPREC, 
                    ACCPERIOD, 
                    ACQDATE, 
                    ADDBY, 
                    ADDDTTM, 
                    ASSVALDATE, 
                    ASSVALKEY, 
                    ASSVALRUNKEY, 
                    BACKDATEADJFLAG, 
                    BGTKEY, 
                    BGTNO, 
                    CAPTLEXP, 
                    DATALAKE_DELETED, 
                    DEPRECAMT, 
                    DEPRECDATE, 
                    DEPRECKEY, 
                    DEPRECPERD, 
                    DEPRECRATE, 
                    DEPRECTYPE, 
                    DEPRECYTD, 
                    DISPSLFLAG, 
                    EFFECTIVEDTTM, 
                    EXPLIFE, 
                    JOURNALDATE, 
                    MODBY, 
                    MODDTTM, 
                    PERIODADJDEPREC, 
                    RECLDEPKEY, 
                    RECLNO, 
                    RESIDLIFE, 
                    RESIDVAL, 
                    REVALFINALCOST, 
                    REVALFLAG, 
                    TOBESOLDFLAG, 
                    VARIATION_ID, 
                    WRITEDNVAL, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCDEPREC, 
                    src.ACCPERIOD, 
                    src.ACQDATE, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ASSVALDATE, 
                    src.ASSVALKEY, 
                    src.ASSVALRUNKEY, 
                    src.BACKDATEADJFLAG, 
                    src.BGTKEY, 
                    src.BGTNO, 
                    src.CAPTLEXP, 
                    src.DATALAKE_DELETED, 
                    src.DEPRECAMT, 
                    src.DEPRECDATE, 
                    src.DEPRECKEY, 
                    src.DEPRECPERD, 
                    src.DEPRECRATE, 
                    src.DEPRECTYPE, 
                    src.DEPRECYTD, 
                    src.DISPSLFLAG, 
                    src.EFFECTIVEDTTM, 
                    src.EXPLIFE, 
                    src.JOURNALDATE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PERIODADJDEPREC, 
                    src.RECLDEPKEY, 
                    src.RECLNO, 
                    src.RESIDLIFE, 
                    src.RESIDVAL, 
                    src.REVALFINALCOST, 
                    src.REVALFLAG, 
                    src.TOBESOLDFLAG, 
                    src.VARIATION_ID, 
                    src.WRITEDNVAL,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_ASSETMANAGEMENT_ASSETVALUATION_ASSDEPRE as target using (
                SELECT * FROM (SELECT 
            strm.ACCDEPREC, 
            strm.ACCPERIOD, 
            strm.ACQDATE, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ASSVALDATE, 
            strm.ASSVALKEY, 
            strm.ASSVALRUNKEY, 
            strm.BACKDATEADJFLAG, 
            strm.BGTKEY, 
            strm.BGTNO, 
            strm.CAPTLEXP, 
            strm.DATALAKE_DELETED, 
            strm.DEPRECAMT, 
            strm.DEPRECDATE, 
            strm.DEPRECKEY, 
            strm.DEPRECPERD, 
            strm.DEPRECRATE, 
            strm.DEPRECTYPE, 
            strm.DEPRECYTD, 
            strm.DISPSLFLAG, 
            strm.EFFECTIVEDTTM, 
            strm.EXPLIFE, 
            strm.JOURNALDATE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PERIODADJDEPREC, 
            strm.RECLDEPKEY, 
            strm.RECLNO, 
            strm.RESIDLIFE, 
            strm.RESIDVAL, 
            strm.REVALFINALCOST, 
            strm.REVALFLAG, 
            strm.TOBESOLDFLAG, 
            strm.VARIATION_ID, 
            strm.WRITEDNVAL, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DEPRECKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSDEPRE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DEPRECKEY=src.DEPRECKEY) OR (target.DEPRECKEY IS NULL AND src.DEPRECKEY IS NULL)) 
                when matched then update set
                    target.ACCDEPREC=src.ACCDEPREC, 
                    target.ACCPERIOD=src.ACCPERIOD, 
                    target.ACQDATE=src.ACQDATE, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ASSVALDATE=src.ASSVALDATE, 
                    target.ASSVALKEY=src.ASSVALKEY, 
                    target.ASSVALRUNKEY=src.ASSVALRUNKEY, 
                    target.BACKDATEADJFLAG=src.BACKDATEADJFLAG, 
                    target.BGTKEY=src.BGTKEY, 
                    target.BGTNO=src.BGTNO, 
                    target.CAPTLEXP=src.CAPTLEXP, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DEPRECAMT=src.DEPRECAMT, 
                    target.DEPRECDATE=src.DEPRECDATE, 
                    target.DEPRECKEY=src.DEPRECKEY, 
                    target.DEPRECPERD=src.DEPRECPERD, 
                    target.DEPRECRATE=src.DEPRECRATE, 
                    target.DEPRECTYPE=src.DEPRECTYPE, 
                    target.DEPRECYTD=src.DEPRECYTD, 
                    target.DISPSLFLAG=src.DISPSLFLAG, 
                    target.EFFECTIVEDTTM=src.EFFECTIVEDTTM, 
                    target.EXPLIFE=src.EXPLIFE, 
                    target.JOURNALDATE=src.JOURNALDATE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PERIODADJDEPREC=src.PERIODADJDEPREC, 
                    target.RECLDEPKEY=src.RECLDEPKEY, 
                    target.RECLNO=src.RECLNO, 
                    target.RESIDLIFE=src.RESIDLIFE, 
                    target.RESIDVAL=src.RESIDVAL, 
                    target.REVALFINALCOST=src.REVALFINALCOST, 
                    target.REVALFLAG=src.REVALFLAG, 
                    target.TOBESOLDFLAG=src.TOBESOLDFLAG, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WRITEDNVAL=src.WRITEDNVAL, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCDEPREC, ACCPERIOD, ACQDATE, ADDBY, ADDDTTM, ASSVALDATE, ASSVALKEY, ASSVALRUNKEY, BACKDATEADJFLAG, BGTKEY, BGTNO, CAPTLEXP, DATALAKE_DELETED, DEPRECAMT, DEPRECDATE, DEPRECKEY, DEPRECPERD, DEPRECRATE, DEPRECTYPE, DEPRECYTD, DISPSLFLAG, EFFECTIVEDTTM, EXPLIFE, JOURNALDATE, MODBY, MODDTTM, PERIODADJDEPREC, RECLDEPKEY, RECLNO, RESIDLIFE, RESIDVAL, REVALFINALCOST, REVALFLAG, TOBESOLDFLAG, VARIATION_ID, WRITEDNVAL, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCDEPREC, 
                    src.ACCPERIOD, 
                    src.ACQDATE, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ASSVALDATE, 
                    src.ASSVALKEY, 
                    src.ASSVALRUNKEY, 
                    src.BACKDATEADJFLAG, 
                    src.BGTKEY, 
                    src.BGTNO, 
                    src.CAPTLEXP, 
                    src.DATALAKE_DELETED, 
                    src.DEPRECAMT, 
                    src.DEPRECDATE, 
                    src.DEPRECKEY, 
                    src.DEPRECPERD, 
                    src.DEPRECRATE, 
                    src.DEPRECTYPE, 
                    src.DEPRECYTD, 
                    src.DISPSLFLAG, 
                    src.EFFECTIVEDTTM, 
                    src.EXPLIFE, 
                    src.JOURNALDATE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PERIODADJDEPREC, 
                    src.RECLDEPKEY, 
                    src.RECLNO, 
                    src.RESIDLIFE, 
                    src.RESIDVAL, 
                    src.REVALFINALCOST, 
                    src.REVALFLAG, 
                    src.TOBESOLDFLAG, 
                    src.VARIATION_ID, 
                    src.WRITEDNVAL,     
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
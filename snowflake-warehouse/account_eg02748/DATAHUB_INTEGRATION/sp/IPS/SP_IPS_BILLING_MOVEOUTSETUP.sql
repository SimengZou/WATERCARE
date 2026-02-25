create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_MOVEOUTSETUP()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_MOVEOUTSETUP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_MOVEOUTSETUP as target using (SELECT * FROM (SELECT 
            strm.ACCTPREFSECTORDER, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BILLINGSTATUS, 
            strm.CANCELCNFRMCNTCTSECTORDER, 
            strm.CANCELCONFIRMATIONMSGKEY, 
            strm.CANCELLOGTYPE, 
            strm.CANCELREQUESTRESOLUTION, 
            strm.CANCELSERVICESSECTORDER, 
            strm.CONFIRMCNTCTSECTORDER, 
            strm.COPYCNTCTSDEFAULTFLAG, 
            strm.COPYCNTCTSPROMPT, 
            strm.COPYWAFROMPREVACCTFLAG, 
            strm.CREATEACCTDEFAULTFLAG, 
            strm.CREATEACCTPROMPT, 
            strm.DELETED, 
            strm.DELINQUENCYSCHEMEKEY, 
            strm.DIRECTDEBITPROMPT, 
            strm.FORWARDINFOPROMPT, 
            strm.FORWARDINFOSECTORDER, 
            strm.FOWARDCNTCTSECTORDER, 
            strm.LOGTYPECODE, 
            strm.MAXDAYSPASTTOALLOWCANCEL, 
            strm.MAXDAYSPASTTOALLOWMODIFY, 
            strm.MAXTENANTINTERVALDAYS, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MODIFYCONFIRMATIONMSGKEY, 
            strm.MODIFYLOGTYPE, 
            strm.MODIFYMAXINTFORNEWREQUEST, 
            strm.MODIFYSERVICESSECTORDER, 
            strm.MODMOVEOUTCNTCTSECTORDER, 
            strm.MOVEINDATEPROMPT, 
            strm.MOVEOUTDATEPROMPT, 
            strm.MOVEOUTINSTRSECTORDER, 
            strm.MOVEOUTSETUPKEY, 
            strm.MOVEOUTSRVINSTFLAG, 
            strm.MOVETOADDRPROMPT, 
            strm.NEWACCTSRVSSECTORDER, 
            strm.NEWSRVFLAG, 
            strm.READINGREQUESTRESOLUTION, 
            strm.SERVICESTATUS, 
            strm.SHOWCANCELCNFRMCNTCTFLAG, 
            strm.SHOWCONFIRMCNTCTINFOFLAG, 
            strm.SHOWMOVEINADDRALRTSFLAG, 
            strm.STAYINAREADEFAULTFLAG, 
            strm.STAYINAREAPROMPT, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MOVEOUTSETUPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_MOVEOUTSETUP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MOVEOUTSETUPKEY=src.MOVEOUTSETUPKEY) OR (target.MOVEOUTSETUPKEY IS NULL AND src.MOVEOUTSETUPKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCTPREFSECTORDER=src.ACCTPREFSECTORDER, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BILLINGSTATUS=src.BILLINGSTATUS, 
                    target.CANCELCNFRMCNTCTSECTORDER=src.CANCELCNFRMCNTCTSECTORDER, 
                    target.CANCELCONFIRMATIONMSGKEY=src.CANCELCONFIRMATIONMSGKEY, 
                    target.CANCELLOGTYPE=src.CANCELLOGTYPE, 
                    target.CANCELREQUESTRESOLUTION=src.CANCELREQUESTRESOLUTION, 
                    target.CANCELSERVICESSECTORDER=src.CANCELSERVICESSECTORDER, 
                    target.CONFIRMCNTCTSECTORDER=src.CONFIRMCNTCTSECTORDER, 
                    target.COPYCNTCTSDEFAULTFLAG=src.COPYCNTCTSDEFAULTFLAG, 
                    target.COPYCNTCTSPROMPT=src.COPYCNTCTSPROMPT, 
                    target.COPYWAFROMPREVACCTFLAG=src.COPYWAFROMPREVACCTFLAG, 
                    target.CREATEACCTDEFAULTFLAG=src.CREATEACCTDEFAULTFLAG, 
                    target.CREATEACCTPROMPT=src.CREATEACCTPROMPT, 
                    target.DELETED=src.DELETED, 
                    target.DELINQUENCYSCHEMEKEY=src.DELINQUENCYSCHEMEKEY, 
                    target.DIRECTDEBITPROMPT=src.DIRECTDEBITPROMPT, 
                    target.FORWARDINFOPROMPT=src.FORWARDINFOPROMPT, 
                    target.FORWARDINFOSECTORDER=src.FORWARDINFOSECTORDER, 
                    target.FOWARDCNTCTSECTORDER=src.FOWARDCNTCTSECTORDER, 
                    target.LOGTYPECODE=src.LOGTYPECODE, 
                    target.MAXDAYSPASTTOALLOWCANCEL=src.MAXDAYSPASTTOALLOWCANCEL, 
                    target.MAXDAYSPASTTOALLOWMODIFY=src.MAXDAYSPASTTOALLOWMODIFY, 
                    target.MAXTENANTINTERVALDAYS=src.MAXTENANTINTERVALDAYS, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MODIFYCONFIRMATIONMSGKEY=src.MODIFYCONFIRMATIONMSGKEY, 
                    target.MODIFYLOGTYPE=src.MODIFYLOGTYPE, 
                    target.MODIFYMAXINTFORNEWREQUEST=src.MODIFYMAXINTFORNEWREQUEST, 
                    target.MODIFYSERVICESSECTORDER=src.MODIFYSERVICESSECTORDER, 
                    target.MODMOVEOUTCNTCTSECTORDER=src.MODMOVEOUTCNTCTSECTORDER, 
                    target.MOVEINDATEPROMPT=src.MOVEINDATEPROMPT, 
                    target.MOVEOUTDATEPROMPT=src.MOVEOUTDATEPROMPT, 
                    target.MOVEOUTINSTRSECTORDER=src.MOVEOUTINSTRSECTORDER, 
                    target.MOVEOUTSETUPKEY=src.MOVEOUTSETUPKEY, 
                    target.MOVEOUTSRVINSTFLAG=src.MOVEOUTSRVINSTFLAG, 
                    target.MOVETOADDRPROMPT=src.MOVETOADDRPROMPT, 
                    target.NEWACCTSRVSSECTORDER=src.NEWACCTSRVSSECTORDER, 
                    target.NEWSRVFLAG=src.NEWSRVFLAG, 
                    target.READINGREQUESTRESOLUTION=src.READINGREQUESTRESOLUTION, 
                    target.SERVICESTATUS=src.SERVICESTATUS, 
                    target.SHOWCANCELCNFRMCNTCTFLAG=src.SHOWCANCELCNFRMCNTCTFLAG, 
                    target.SHOWCONFIRMCNTCTINFOFLAG=src.SHOWCONFIRMCNTCTINFOFLAG, 
                    target.SHOWMOVEINADDRALRTSFLAG=src.SHOWMOVEINADDRALRTSFLAG, 
                    target.STAYINAREADEFAULTFLAG=src.STAYINAREADEFAULTFLAG, 
                    target.STAYINAREAPROMPT=src.STAYINAREAPROMPT, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCTPREFSECTORDER, 
                    ADDBY, 
                    ADDDTTM, 
                    BILLINGSTATUS, 
                    CANCELCNFRMCNTCTSECTORDER, 
                    CANCELCONFIRMATIONMSGKEY, 
                    CANCELLOGTYPE, 
                    CANCELREQUESTRESOLUTION, 
                    CANCELSERVICESSECTORDER, 
                    CONFIRMCNTCTSECTORDER, 
                    COPYCNTCTSDEFAULTFLAG, 
                    COPYCNTCTSPROMPT, 
                    COPYWAFROMPREVACCTFLAG, 
                    CREATEACCTDEFAULTFLAG, 
                    CREATEACCTPROMPT, 
                    DELETED, 
                    DELINQUENCYSCHEMEKEY, 
                    DIRECTDEBITPROMPT, 
                    FORWARDINFOPROMPT, 
                    FORWARDINFOSECTORDER, 
                    FOWARDCNTCTSECTORDER, 
                    LOGTYPECODE, 
                    MAXDAYSPASTTOALLOWCANCEL, 
                    MAXDAYSPASTTOALLOWMODIFY, 
                    MAXTENANTINTERVALDAYS, 
                    MODBY, 
                    MODDTTM, 
                    MODIFYCONFIRMATIONMSGKEY, 
                    MODIFYLOGTYPE, 
                    MODIFYMAXINTFORNEWREQUEST, 
                    MODIFYSERVICESSECTORDER, 
                    MODMOVEOUTCNTCTSECTORDER, 
                    MOVEINDATEPROMPT, 
                    MOVEOUTDATEPROMPT, 
                    MOVEOUTINSTRSECTORDER, 
                    MOVEOUTSETUPKEY, 
                    MOVEOUTSRVINSTFLAG, 
                    MOVETOADDRPROMPT, 
                    NEWACCTSRVSSECTORDER, 
                    NEWSRVFLAG, 
                    READINGREQUESTRESOLUTION, 
                    SERVICESTATUS, 
                    SHOWCANCELCNFRMCNTCTFLAG, 
                    SHOWCONFIRMCNTCTINFOFLAG, 
                    SHOWMOVEINADDRALRTSFLAG, 
                    STAYINAREADEFAULTFLAG, 
                    STAYINAREAPROMPT, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCTPREFSECTORDER, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BILLINGSTATUS, 
                    src.CANCELCNFRMCNTCTSECTORDER, 
                    src.CANCELCONFIRMATIONMSGKEY, 
                    src.CANCELLOGTYPE, 
                    src.CANCELREQUESTRESOLUTION, 
                    src.CANCELSERVICESSECTORDER, 
                    src.CONFIRMCNTCTSECTORDER, 
                    src.COPYCNTCTSDEFAULTFLAG, 
                    src.COPYCNTCTSPROMPT, 
                    src.COPYWAFROMPREVACCTFLAG, 
                    src.CREATEACCTDEFAULTFLAG, 
                    src.CREATEACCTPROMPT, 
                    src.DELETED, 
                    src.DELINQUENCYSCHEMEKEY, 
                    src.DIRECTDEBITPROMPT, 
                    src.FORWARDINFOPROMPT, 
                    src.FORWARDINFOSECTORDER, 
                    src.FOWARDCNTCTSECTORDER, 
                    src.LOGTYPECODE, 
                    src.MAXDAYSPASTTOALLOWCANCEL, 
                    src.MAXDAYSPASTTOALLOWMODIFY, 
                    src.MAXTENANTINTERVALDAYS, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MODIFYCONFIRMATIONMSGKEY, 
                    src.MODIFYLOGTYPE, 
                    src.MODIFYMAXINTFORNEWREQUEST, 
                    src.MODIFYSERVICESSECTORDER, 
                    src.MODMOVEOUTCNTCTSECTORDER, 
                    src.MOVEINDATEPROMPT, 
                    src.MOVEOUTDATEPROMPT, 
                    src.MOVEOUTINSTRSECTORDER, 
                    src.MOVEOUTSETUPKEY, 
                    src.MOVEOUTSRVINSTFLAG, 
                    src.MOVETOADDRPROMPT, 
                    src.NEWACCTSRVSSECTORDER, 
                    src.NEWSRVFLAG, 
                    src.READINGREQUESTRESOLUTION, 
                    src.SERVICESTATUS, 
                    src.SHOWCANCELCNFRMCNTCTFLAG, 
                    src.SHOWCONFIRMCNTCTINFOFLAG, 
                    src.SHOWMOVEINADDRALRTSFLAG, 
                    src.STAYINAREADEFAULTFLAG, 
                    src.STAYINAREAPROMPT, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_MOVEOUTSETUP as target using (
                SELECT * FROM (SELECT 
            strm.ACCTPREFSECTORDER, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BILLINGSTATUS, 
            strm.CANCELCNFRMCNTCTSECTORDER, 
            strm.CANCELCONFIRMATIONMSGKEY, 
            strm.CANCELLOGTYPE, 
            strm.CANCELREQUESTRESOLUTION, 
            strm.CANCELSERVICESSECTORDER, 
            strm.CONFIRMCNTCTSECTORDER, 
            strm.COPYCNTCTSDEFAULTFLAG, 
            strm.COPYCNTCTSPROMPT, 
            strm.COPYWAFROMPREVACCTFLAG, 
            strm.CREATEACCTDEFAULTFLAG, 
            strm.CREATEACCTPROMPT, 
            strm.DELETED, 
            strm.DELINQUENCYSCHEMEKEY, 
            strm.DIRECTDEBITPROMPT, 
            strm.FORWARDINFOPROMPT, 
            strm.FORWARDINFOSECTORDER, 
            strm.FOWARDCNTCTSECTORDER, 
            strm.LOGTYPECODE, 
            strm.MAXDAYSPASTTOALLOWCANCEL, 
            strm.MAXDAYSPASTTOALLOWMODIFY, 
            strm.MAXTENANTINTERVALDAYS, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MODIFYCONFIRMATIONMSGKEY, 
            strm.MODIFYLOGTYPE, 
            strm.MODIFYMAXINTFORNEWREQUEST, 
            strm.MODIFYSERVICESSECTORDER, 
            strm.MODMOVEOUTCNTCTSECTORDER, 
            strm.MOVEINDATEPROMPT, 
            strm.MOVEOUTDATEPROMPT, 
            strm.MOVEOUTINSTRSECTORDER, 
            strm.MOVEOUTSETUPKEY, 
            strm.MOVEOUTSRVINSTFLAG, 
            strm.MOVETOADDRPROMPT, 
            strm.NEWACCTSRVSSECTORDER, 
            strm.NEWSRVFLAG, 
            strm.READINGREQUESTRESOLUTION, 
            strm.SERVICESTATUS, 
            strm.SHOWCANCELCNFRMCNTCTFLAG, 
            strm.SHOWCONFIRMCNTCTINFOFLAG, 
            strm.SHOWMOVEINADDRALRTSFLAG, 
            strm.STAYINAREADEFAULTFLAG, 
            strm.STAYINAREAPROMPT, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MOVEOUTSETUPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_MOVEOUTSETUP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MOVEOUTSETUPKEY=src.MOVEOUTSETUPKEY) OR (target.MOVEOUTSETUPKEY IS NULL AND src.MOVEOUTSETUPKEY IS NULL)) 
                when matched then update set
                    target.ACCTPREFSECTORDER=src.ACCTPREFSECTORDER, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BILLINGSTATUS=src.BILLINGSTATUS, 
                    target.CANCELCNFRMCNTCTSECTORDER=src.CANCELCNFRMCNTCTSECTORDER, 
                    target.CANCELCONFIRMATIONMSGKEY=src.CANCELCONFIRMATIONMSGKEY, 
                    target.CANCELLOGTYPE=src.CANCELLOGTYPE, 
                    target.CANCELREQUESTRESOLUTION=src.CANCELREQUESTRESOLUTION, 
                    target.CANCELSERVICESSECTORDER=src.CANCELSERVICESSECTORDER, 
                    target.CONFIRMCNTCTSECTORDER=src.CONFIRMCNTCTSECTORDER, 
                    target.COPYCNTCTSDEFAULTFLAG=src.COPYCNTCTSDEFAULTFLAG, 
                    target.COPYCNTCTSPROMPT=src.COPYCNTCTSPROMPT, 
                    target.COPYWAFROMPREVACCTFLAG=src.COPYWAFROMPREVACCTFLAG, 
                    target.CREATEACCTDEFAULTFLAG=src.CREATEACCTDEFAULTFLAG, 
                    target.CREATEACCTPROMPT=src.CREATEACCTPROMPT, 
                    target.DELETED=src.DELETED, 
                    target.DELINQUENCYSCHEMEKEY=src.DELINQUENCYSCHEMEKEY, 
                    target.DIRECTDEBITPROMPT=src.DIRECTDEBITPROMPT, 
                    target.FORWARDINFOPROMPT=src.FORWARDINFOPROMPT, 
                    target.FORWARDINFOSECTORDER=src.FORWARDINFOSECTORDER, 
                    target.FOWARDCNTCTSECTORDER=src.FOWARDCNTCTSECTORDER, 
                    target.LOGTYPECODE=src.LOGTYPECODE, 
                    target.MAXDAYSPASTTOALLOWCANCEL=src.MAXDAYSPASTTOALLOWCANCEL, 
                    target.MAXDAYSPASTTOALLOWMODIFY=src.MAXDAYSPASTTOALLOWMODIFY, 
                    target.MAXTENANTINTERVALDAYS=src.MAXTENANTINTERVALDAYS, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MODIFYCONFIRMATIONMSGKEY=src.MODIFYCONFIRMATIONMSGKEY, 
                    target.MODIFYLOGTYPE=src.MODIFYLOGTYPE, 
                    target.MODIFYMAXINTFORNEWREQUEST=src.MODIFYMAXINTFORNEWREQUEST, 
                    target.MODIFYSERVICESSECTORDER=src.MODIFYSERVICESSECTORDER, 
                    target.MODMOVEOUTCNTCTSECTORDER=src.MODMOVEOUTCNTCTSECTORDER, 
                    target.MOVEINDATEPROMPT=src.MOVEINDATEPROMPT, 
                    target.MOVEOUTDATEPROMPT=src.MOVEOUTDATEPROMPT, 
                    target.MOVEOUTINSTRSECTORDER=src.MOVEOUTINSTRSECTORDER, 
                    target.MOVEOUTSETUPKEY=src.MOVEOUTSETUPKEY, 
                    target.MOVEOUTSRVINSTFLAG=src.MOVEOUTSRVINSTFLAG, 
                    target.MOVETOADDRPROMPT=src.MOVETOADDRPROMPT, 
                    target.NEWACCTSRVSSECTORDER=src.NEWACCTSRVSSECTORDER, 
                    target.NEWSRVFLAG=src.NEWSRVFLAG, 
                    target.READINGREQUESTRESOLUTION=src.READINGREQUESTRESOLUTION, 
                    target.SERVICESTATUS=src.SERVICESTATUS, 
                    target.SHOWCANCELCNFRMCNTCTFLAG=src.SHOWCANCELCNFRMCNTCTFLAG, 
                    target.SHOWCONFIRMCNTCTINFOFLAG=src.SHOWCONFIRMCNTCTINFOFLAG, 
                    target.SHOWMOVEINADDRALRTSFLAG=src.SHOWMOVEINADDRALRTSFLAG, 
                    target.STAYINAREADEFAULTFLAG=src.STAYINAREADEFAULTFLAG, 
                    target.STAYINAREAPROMPT=src.STAYINAREAPROMPT, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCTPREFSECTORDER, ADDBY, ADDDTTM, BILLINGSTATUS, CANCELCNFRMCNTCTSECTORDER, CANCELCONFIRMATIONMSGKEY, CANCELLOGTYPE, CANCELREQUESTRESOLUTION, CANCELSERVICESSECTORDER, CONFIRMCNTCTSECTORDER, COPYCNTCTSDEFAULTFLAG, COPYCNTCTSPROMPT, COPYWAFROMPREVACCTFLAG, CREATEACCTDEFAULTFLAG, CREATEACCTPROMPT, DELETED, DELINQUENCYSCHEMEKEY, DIRECTDEBITPROMPT, FORWARDINFOPROMPT, FORWARDINFOSECTORDER, FOWARDCNTCTSECTORDER, LOGTYPECODE, MAXDAYSPASTTOALLOWCANCEL, MAXDAYSPASTTOALLOWMODIFY, MAXTENANTINTERVALDAYS, MODBY, MODDTTM, MODIFYCONFIRMATIONMSGKEY, MODIFYLOGTYPE, MODIFYMAXINTFORNEWREQUEST, MODIFYSERVICESSECTORDER, MODMOVEOUTCNTCTSECTORDER, MOVEINDATEPROMPT, MOVEOUTDATEPROMPT, MOVEOUTINSTRSECTORDER, MOVEOUTSETUPKEY, MOVEOUTSRVINSTFLAG, MOVETOADDRPROMPT, NEWACCTSRVSSECTORDER, NEWSRVFLAG, READINGREQUESTRESOLUTION, SERVICESTATUS, SHOWCANCELCNFRMCNTCTFLAG, SHOWCONFIRMCNTCTINFOFLAG, SHOWMOVEINADDRALRTSFLAG, STAYINAREADEFAULTFLAG, STAYINAREAPROMPT, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCTPREFSECTORDER, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BILLINGSTATUS, 
                    src.CANCELCNFRMCNTCTSECTORDER, 
                    src.CANCELCONFIRMATIONMSGKEY, 
                    src.CANCELLOGTYPE, 
                    src.CANCELREQUESTRESOLUTION, 
                    src.CANCELSERVICESSECTORDER, 
                    src.CONFIRMCNTCTSECTORDER, 
                    src.COPYCNTCTSDEFAULTFLAG, 
                    src.COPYCNTCTSPROMPT, 
                    src.COPYWAFROMPREVACCTFLAG, 
                    src.CREATEACCTDEFAULTFLAG, 
                    src.CREATEACCTPROMPT, 
                    src.DELETED, 
                    src.DELINQUENCYSCHEMEKEY, 
                    src.DIRECTDEBITPROMPT, 
                    src.FORWARDINFOPROMPT, 
                    src.FORWARDINFOSECTORDER, 
                    src.FOWARDCNTCTSECTORDER, 
                    src.LOGTYPECODE, 
                    src.MAXDAYSPASTTOALLOWCANCEL, 
                    src.MAXDAYSPASTTOALLOWMODIFY, 
                    src.MAXTENANTINTERVALDAYS, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MODIFYCONFIRMATIONMSGKEY, 
                    src.MODIFYLOGTYPE, 
                    src.MODIFYMAXINTFORNEWREQUEST, 
                    src.MODIFYSERVICESSECTORDER, 
                    src.MODMOVEOUTCNTCTSECTORDER, 
                    src.MOVEINDATEPROMPT, 
                    src.MOVEOUTDATEPROMPT, 
                    src.MOVEOUTINSTRSECTORDER, 
                    src.MOVEOUTSETUPKEY, 
                    src.MOVEOUTSRVINSTFLAG, 
                    src.MOVETOADDRPROMPT, 
                    src.NEWACCTSRVSSECTORDER, 
                    src.NEWSRVFLAG, 
                    src.READINGREQUESTRESOLUTION, 
                    src.SERVICESTATUS, 
                    src.SHOWCANCELCNFRMCNTCTFLAG, 
                    src.SHOWCONFIRMCNTCTINFOFLAG, 
                    src.SHOWMOVEINADDRALRTSFLAG, 
                    src.STAYINAREADEFAULTFLAG, 
                    src.STAYINAREAPROMPT, 
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
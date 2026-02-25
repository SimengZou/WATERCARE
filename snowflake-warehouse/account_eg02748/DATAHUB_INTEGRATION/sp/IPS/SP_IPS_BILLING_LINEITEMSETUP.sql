create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_LINEITEMSETUP()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_LINEITEMSETUP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_LINEITEMSETUP as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPLYDEPOSITFLAG, 
            strm.APPROVALGROUP, 
            strm.APPROVALSCHEME, 
            strm.AUTOMATEDFLAG, 
            strm.BBSETLFLAG, 
            strm.BILLINGFAMILY, 
            strm.BUDGETBILLINGVARIANTFLAG, 
            strm.COMMENTSKEY, 
            strm.CONDITIONFORMULAKEY, 
            strm.DEFAULTADJUSTMENTSETUPKEY, 
            strm.DEFAULTQUANTITY, 
            strm.DELETED, 
            strm.DELINQUENCYEXEMPTFLAG, 
            strm.DESCRIPTION, 
            strm.DESIGNATION, 
            strm.DONATIONFLAG, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.FEETYPEKEY, 
            strm.FORTRANSFERFLAG, 
            strm.FORTRANSFERPAYMENTFLAG, 
            strm.INCLUDEINBUDGETEDAMOUNTFLAG, 
            strm.INITIALDEPOSITCHARGEFLAG, 
            strm.ISPRINTTEXTEDITABLE, 
            strm.LINEITEM, 
            strm.LINEITEMGROUP, 
            strm.LINEITEMSETUPKEY, 
            strm.LINEITEMTYPE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MULTIDISTRIBUTIOLNALFLAG, 
            strm.NUMBEROFPROMPTPAIDBILLS, 
            strm.ONEOFFOPTIONINDICATOR, 
            strm.PENALTYCODEKEY, 
            strm.PRINTTEXT, 
            strm.PROMPTPAIDPERIOD, 
            strm.PROPERTYBASEDFLAG, 
            strm.QUANTITYBASEDFLAG, 
            strm.RATECHANGEALGORITHM, 
            strm.RATECODEKEY, 
            strm.REBATEGROUP, 
            strm.ROUNDTO, 
            strm.SERVICEOPTIONSKEY, 
            strm.SETTLEMENTLINEITEMSETUPKEY, 
            strm.SRVBASEDOPTIONINDICATOR, 
            strm.TAXAREA, 
            strm.TAXINCLUSIVEFLAG, 
            strm.TYPEINDICATOR, 
            strm.VALUEBASEDFLAG, 
            strm.VALUEFORMULAKEY, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.LINEITEMSETUPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_LINEITEMSETUP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.LINEITEMSETUPKEY=src.LINEITEMSETUPKEY) OR (target.LINEITEMSETUPKEY IS NULL AND src.LINEITEMSETUPKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPLYDEPOSITFLAG=src.APPLYDEPOSITFLAG, 
                    target.APPROVALGROUP=src.APPROVALGROUP, 
                    target.APPROVALSCHEME=src.APPROVALSCHEME, 
                    target.AUTOMATEDFLAG=src.AUTOMATEDFLAG, 
                    target.BBSETLFLAG=src.BBSETLFLAG, 
                    target.BILLINGFAMILY=src.BILLINGFAMILY, 
                    target.BUDGETBILLINGVARIANTFLAG=src.BUDGETBILLINGVARIANTFLAG, 
                    target.COMMENTSKEY=src.COMMENTSKEY, 
                    target.CONDITIONFORMULAKEY=src.CONDITIONFORMULAKEY, 
                    target.DEFAULTADJUSTMENTSETUPKEY=src.DEFAULTADJUSTMENTSETUPKEY, 
                    target.DEFAULTQUANTITY=src.DEFAULTQUANTITY, 
                    target.DELETED=src.DELETED, 
                    target.DELINQUENCYEXEMPTFLAG=src.DELINQUENCYEXEMPTFLAG, 
                    target.DESCRIPTION=src.DESCRIPTION, 
                    target.DESIGNATION=src.DESIGNATION, 
                    target.DONATIONFLAG=src.DONATIONFLAG, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.FEETYPEKEY=src.FEETYPEKEY, 
                    target.FORTRANSFERFLAG=src.FORTRANSFERFLAG, 
                    target.FORTRANSFERPAYMENTFLAG=src.FORTRANSFERPAYMENTFLAG, 
                    target.INCLUDEINBUDGETEDAMOUNTFLAG=src.INCLUDEINBUDGETEDAMOUNTFLAG, 
                    target.INITIALDEPOSITCHARGEFLAG=src.INITIALDEPOSITCHARGEFLAG, 
                    target.ISPRINTTEXTEDITABLE=src.ISPRINTTEXTEDITABLE, 
                    target.LINEITEM=src.LINEITEM, 
                    target.LINEITEMGROUP=src.LINEITEMGROUP, 
                    target.LINEITEMSETUPKEY=src.LINEITEMSETUPKEY, 
                    target.LINEITEMTYPE=src.LINEITEMTYPE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MULTIDISTRIBUTIOLNALFLAG=src.MULTIDISTRIBUTIOLNALFLAG, 
                    target.NUMBEROFPROMPTPAIDBILLS=src.NUMBEROFPROMPTPAIDBILLS, 
                    target.ONEOFFOPTIONINDICATOR=src.ONEOFFOPTIONINDICATOR, 
                    target.PENALTYCODEKEY=src.PENALTYCODEKEY, 
                    target.PRINTTEXT=src.PRINTTEXT, 
                    target.PROMPTPAIDPERIOD=src.PROMPTPAIDPERIOD, 
                    target.PROPERTYBASEDFLAG=src.PROPERTYBASEDFLAG, 
                    target.QUANTITYBASEDFLAG=src.QUANTITYBASEDFLAG, 
                    target.RATECHANGEALGORITHM=src.RATECHANGEALGORITHM, 
                    target.RATECODEKEY=src.RATECODEKEY, 
                    target.REBATEGROUP=src.REBATEGROUP, 
                    target.ROUNDTO=src.ROUNDTO, 
                    target.SERVICEOPTIONSKEY=src.SERVICEOPTIONSKEY, 
                    target.SETTLEMENTLINEITEMSETUPKEY=src.SETTLEMENTLINEITEMSETUPKEY, 
                    target.SRVBASEDOPTIONINDICATOR=src.SRVBASEDOPTIONINDICATOR, 
                    target.TAXAREA=src.TAXAREA, 
                    target.TAXINCLUSIVEFLAG=src.TAXINCLUSIVEFLAG, 
                    target.TYPEINDICATOR=src.TYPEINDICATOR, 
                    target.VALUEBASEDFLAG=src.VALUEBASEDFLAG, 
                    target.VALUEFORMULAKEY=src.VALUEFORMULAKEY, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    APPLYDEPOSITFLAG, 
                    APPROVALGROUP, 
                    APPROVALSCHEME, 
                    AUTOMATEDFLAG, 
                    BBSETLFLAG, 
                    BILLINGFAMILY, 
                    BUDGETBILLINGVARIANTFLAG, 
                    COMMENTSKEY, 
                    CONDITIONFORMULAKEY, 
                    DEFAULTADJUSTMENTSETUPKEY, 
                    DEFAULTQUANTITY, 
                    DELETED, 
                    DELINQUENCYEXEMPTFLAG, 
                    DESCRIPTION, 
                    DESIGNATION, 
                    DONATIONFLAG, 
                    EFFDATE, 
                    EXPDATE, 
                    FEETYPEKEY, 
                    FORTRANSFERFLAG, 
                    FORTRANSFERPAYMENTFLAG, 
                    INCLUDEINBUDGETEDAMOUNTFLAG, 
                    INITIALDEPOSITCHARGEFLAG, 
                    ISPRINTTEXTEDITABLE, 
                    LINEITEM, 
                    LINEITEMGROUP, 
                    LINEITEMSETUPKEY, 
                    LINEITEMTYPE, 
                    MODBY, 
                    MODDTTM, 
                    MULTIDISTRIBUTIOLNALFLAG, 
                    NUMBEROFPROMPTPAIDBILLS, 
                    ONEOFFOPTIONINDICATOR, 
                    PENALTYCODEKEY, 
                    PRINTTEXT, 
                    PROMPTPAIDPERIOD, 
                    PROPERTYBASEDFLAG, 
                    QUANTITYBASEDFLAG, 
                    RATECHANGEALGORITHM, 
                    RATECODEKEY, 
                    REBATEGROUP, 
                    ROUNDTO, 
                    SERVICEOPTIONSKEY, 
                    SETTLEMENTLINEITEMSETUPKEY, 
                    SRVBASEDOPTIONINDICATOR, 
                    TAXAREA, 
                    TAXINCLUSIVEFLAG, 
                    TYPEINDICATOR, 
                    VALUEBASEDFLAG, 
                    VALUEFORMULAKEY, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPLYDEPOSITFLAG, 
                    src.APPROVALGROUP, 
                    src.APPROVALSCHEME, 
                    src.AUTOMATEDFLAG, 
                    src.BBSETLFLAG, 
                    src.BILLINGFAMILY, 
                    src.BUDGETBILLINGVARIANTFLAG, 
                    src.COMMENTSKEY, 
                    src.CONDITIONFORMULAKEY, 
                    src.DEFAULTADJUSTMENTSETUPKEY, 
                    src.DEFAULTQUANTITY, 
                    src.DELETED, 
                    src.DELINQUENCYEXEMPTFLAG, 
                    src.DESCRIPTION, 
                    src.DESIGNATION, 
                    src.DONATIONFLAG, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.FEETYPEKEY, 
                    src.FORTRANSFERFLAG, 
                    src.FORTRANSFERPAYMENTFLAG, 
                    src.INCLUDEINBUDGETEDAMOUNTFLAG, 
                    src.INITIALDEPOSITCHARGEFLAG, 
                    src.ISPRINTTEXTEDITABLE, 
                    src.LINEITEM, 
                    src.LINEITEMGROUP, 
                    src.LINEITEMSETUPKEY, 
                    src.LINEITEMTYPE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MULTIDISTRIBUTIOLNALFLAG, 
                    src.NUMBEROFPROMPTPAIDBILLS, 
                    src.ONEOFFOPTIONINDICATOR, 
                    src.PENALTYCODEKEY, 
                    src.PRINTTEXT, 
                    src.PROMPTPAIDPERIOD, 
                    src.PROPERTYBASEDFLAG, 
                    src.QUANTITYBASEDFLAG, 
                    src.RATECHANGEALGORITHM, 
                    src.RATECODEKEY, 
                    src.REBATEGROUP, 
                    src.ROUNDTO, 
                    src.SERVICEOPTIONSKEY, 
                    src.SETTLEMENTLINEITEMSETUPKEY, 
                    src.SRVBASEDOPTIONINDICATOR, 
                    src.TAXAREA, 
                    src.TAXINCLUSIVEFLAG, 
                    src.TYPEINDICATOR, 
                    src.VALUEBASEDFLAG, 
                    src.VALUEFORMULAKEY, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_LINEITEMSETUP as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPLYDEPOSITFLAG, 
            strm.APPROVALGROUP, 
            strm.APPROVALSCHEME, 
            strm.AUTOMATEDFLAG, 
            strm.BBSETLFLAG, 
            strm.BILLINGFAMILY, 
            strm.BUDGETBILLINGVARIANTFLAG, 
            strm.COMMENTSKEY, 
            strm.CONDITIONFORMULAKEY, 
            strm.DEFAULTADJUSTMENTSETUPKEY, 
            strm.DEFAULTQUANTITY, 
            strm.DELETED, 
            strm.DELINQUENCYEXEMPTFLAG, 
            strm.DESCRIPTION, 
            strm.DESIGNATION, 
            strm.DONATIONFLAG, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.FEETYPEKEY, 
            strm.FORTRANSFERFLAG, 
            strm.FORTRANSFERPAYMENTFLAG, 
            strm.INCLUDEINBUDGETEDAMOUNTFLAG, 
            strm.INITIALDEPOSITCHARGEFLAG, 
            strm.ISPRINTTEXTEDITABLE, 
            strm.LINEITEM, 
            strm.LINEITEMGROUP, 
            strm.LINEITEMSETUPKEY, 
            strm.LINEITEMTYPE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MULTIDISTRIBUTIOLNALFLAG, 
            strm.NUMBEROFPROMPTPAIDBILLS, 
            strm.ONEOFFOPTIONINDICATOR, 
            strm.PENALTYCODEKEY, 
            strm.PRINTTEXT, 
            strm.PROMPTPAIDPERIOD, 
            strm.PROPERTYBASEDFLAG, 
            strm.QUANTITYBASEDFLAG, 
            strm.RATECHANGEALGORITHM, 
            strm.RATECODEKEY, 
            strm.REBATEGROUP, 
            strm.ROUNDTO, 
            strm.SERVICEOPTIONSKEY, 
            strm.SETTLEMENTLINEITEMSETUPKEY, 
            strm.SRVBASEDOPTIONINDICATOR, 
            strm.TAXAREA, 
            strm.TAXINCLUSIVEFLAG, 
            strm.TYPEINDICATOR, 
            strm.VALUEBASEDFLAG, 
            strm.VALUEFORMULAKEY, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.LINEITEMSETUPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_LINEITEMSETUP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.LINEITEMSETUPKEY=src.LINEITEMSETUPKEY) OR (target.LINEITEMSETUPKEY IS NULL AND src.LINEITEMSETUPKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPLYDEPOSITFLAG=src.APPLYDEPOSITFLAG, 
                    target.APPROVALGROUP=src.APPROVALGROUP, 
                    target.APPROVALSCHEME=src.APPROVALSCHEME, 
                    target.AUTOMATEDFLAG=src.AUTOMATEDFLAG, 
                    target.BBSETLFLAG=src.BBSETLFLAG, 
                    target.BILLINGFAMILY=src.BILLINGFAMILY, 
                    target.BUDGETBILLINGVARIANTFLAG=src.BUDGETBILLINGVARIANTFLAG, 
                    target.COMMENTSKEY=src.COMMENTSKEY, 
                    target.CONDITIONFORMULAKEY=src.CONDITIONFORMULAKEY, 
                    target.DEFAULTADJUSTMENTSETUPKEY=src.DEFAULTADJUSTMENTSETUPKEY, 
                    target.DEFAULTQUANTITY=src.DEFAULTQUANTITY, 
                    target.DELETED=src.DELETED, 
                    target.DELINQUENCYEXEMPTFLAG=src.DELINQUENCYEXEMPTFLAG, 
                    target.DESCRIPTION=src.DESCRIPTION, 
                    target.DESIGNATION=src.DESIGNATION, 
                    target.DONATIONFLAG=src.DONATIONFLAG, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.FEETYPEKEY=src.FEETYPEKEY, 
                    target.FORTRANSFERFLAG=src.FORTRANSFERFLAG, 
                    target.FORTRANSFERPAYMENTFLAG=src.FORTRANSFERPAYMENTFLAG, 
                    target.INCLUDEINBUDGETEDAMOUNTFLAG=src.INCLUDEINBUDGETEDAMOUNTFLAG, 
                    target.INITIALDEPOSITCHARGEFLAG=src.INITIALDEPOSITCHARGEFLAG, 
                    target.ISPRINTTEXTEDITABLE=src.ISPRINTTEXTEDITABLE, 
                    target.LINEITEM=src.LINEITEM, 
                    target.LINEITEMGROUP=src.LINEITEMGROUP, 
                    target.LINEITEMSETUPKEY=src.LINEITEMSETUPKEY, 
                    target.LINEITEMTYPE=src.LINEITEMTYPE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MULTIDISTRIBUTIOLNALFLAG=src.MULTIDISTRIBUTIOLNALFLAG, 
                    target.NUMBEROFPROMPTPAIDBILLS=src.NUMBEROFPROMPTPAIDBILLS, 
                    target.ONEOFFOPTIONINDICATOR=src.ONEOFFOPTIONINDICATOR, 
                    target.PENALTYCODEKEY=src.PENALTYCODEKEY, 
                    target.PRINTTEXT=src.PRINTTEXT, 
                    target.PROMPTPAIDPERIOD=src.PROMPTPAIDPERIOD, 
                    target.PROPERTYBASEDFLAG=src.PROPERTYBASEDFLAG, 
                    target.QUANTITYBASEDFLAG=src.QUANTITYBASEDFLAG, 
                    target.RATECHANGEALGORITHM=src.RATECHANGEALGORITHM, 
                    target.RATECODEKEY=src.RATECODEKEY, 
                    target.REBATEGROUP=src.REBATEGROUP, 
                    target.ROUNDTO=src.ROUNDTO, 
                    target.SERVICEOPTIONSKEY=src.SERVICEOPTIONSKEY, 
                    target.SETTLEMENTLINEITEMSETUPKEY=src.SETTLEMENTLINEITEMSETUPKEY, 
                    target.SRVBASEDOPTIONINDICATOR=src.SRVBASEDOPTIONINDICATOR, 
                    target.TAXAREA=src.TAXAREA, 
                    target.TAXINCLUSIVEFLAG=src.TAXINCLUSIVEFLAG, 
                    target.TYPEINDICATOR=src.TYPEINDICATOR, 
                    target.VALUEBASEDFLAG=src.VALUEBASEDFLAG, 
                    target.VALUEFORMULAKEY=src.VALUEFORMULAKEY, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, APPLYDEPOSITFLAG, APPROVALGROUP, APPROVALSCHEME, AUTOMATEDFLAG, BBSETLFLAG, BILLINGFAMILY, BUDGETBILLINGVARIANTFLAG, COMMENTSKEY, CONDITIONFORMULAKEY, DEFAULTADJUSTMENTSETUPKEY, DEFAULTQUANTITY, DELETED, DELINQUENCYEXEMPTFLAG, DESCRIPTION, DESIGNATION, DONATIONFLAG, EFFDATE, EXPDATE, FEETYPEKEY, FORTRANSFERFLAG, FORTRANSFERPAYMENTFLAG, INCLUDEINBUDGETEDAMOUNTFLAG, INITIALDEPOSITCHARGEFLAG, ISPRINTTEXTEDITABLE, LINEITEM, LINEITEMGROUP, LINEITEMSETUPKEY, LINEITEMTYPE, MODBY, MODDTTM, MULTIDISTRIBUTIOLNALFLAG, NUMBEROFPROMPTPAIDBILLS, ONEOFFOPTIONINDICATOR, PENALTYCODEKEY, PRINTTEXT, PROMPTPAIDPERIOD, PROPERTYBASEDFLAG, QUANTITYBASEDFLAG, RATECHANGEALGORITHM, RATECODEKEY, REBATEGROUP, ROUNDTO, SERVICEOPTIONSKEY, SETTLEMENTLINEITEMSETUPKEY, SRVBASEDOPTIONINDICATOR, TAXAREA, TAXINCLUSIVEFLAG, TYPEINDICATOR, VALUEBASEDFLAG, VALUEFORMULAKEY, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPLYDEPOSITFLAG, 
                    src.APPROVALGROUP, 
                    src.APPROVALSCHEME, 
                    src.AUTOMATEDFLAG, 
                    src.BBSETLFLAG, 
                    src.BILLINGFAMILY, 
                    src.BUDGETBILLINGVARIANTFLAG, 
                    src.COMMENTSKEY, 
                    src.CONDITIONFORMULAKEY, 
                    src.DEFAULTADJUSTMENTSETUPKEY, 
                    src.DEFAULTQUANTITY, 
                    src.DELETED, 
                    src.DELINQUENCYEXEMPTFLAG, 
                    src.DESCRIPTION, 
                    src.DESIGNATION, 
                    src.DONATIONFLAG, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.FEETYPEKEY, 
                    src.FORTRANSFERFLAG, 
                    src.FORTRANSFERPAYMENTFLAG, 
                    src.INCLUDEINBUDGETEDAMOUNTFLAG, 
                    src.INITIALDEPOSITCHARGEFLAG, 
                    src.ISPRINTTEXTEDITABLE, 
                    src.LINEITEM, 
                    src.LINEITEMGROUP, 
                    src.LINEITEMSETUPKEY, 
                    src.LINEITEMTYPE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MULTIDISTRIBUTIOLNALFLAG, 
                    src.NUMBEROFPROMPTPAIDBILLS, 
                    src.ONEOFFOPTIONINDICATOR, 
                    src.PENALTYCODEKEY, 
                    src.PRINTTEXT, 
                    src.PROMPTPAIDPERIOD, 
                    src.PROPERTYBASEDFLAG, 
                    src.QUANTITYBASEDFLAG, 
                    src.RATECHANGEALGORITHM, 
                    src.RATECODEKEY, 
                    src.REBATEGROUP, 
                    src.ROUNDTO, 
                    src.SERVICEOPTIONSKEY, 
                    src.SETTLEMENTLINEITEMSETUPKEY, 
                    src.SRVBASEDOPTIONINDICATOR, 
                    src.TAXAREA, 
                    src.TAXINCLUSIVEFLAG, 
                    src.TYPEINDICATOR, 
                    src.VALUEBASEDFLAG, 
                    src.VALUEFORMULAKEY, 
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
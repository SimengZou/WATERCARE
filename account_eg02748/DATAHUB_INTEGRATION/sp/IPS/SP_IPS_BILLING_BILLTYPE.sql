create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_BILLTYPE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_BILLTYPE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_BILLTYPE as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPLYCREDITTYPE, 
            strm.BILLDESC, 
            strm.BILLINGFAMILY, 
            strm.BILLMAXIMUM, 
            strm.BILLMINIMUM, 
            strm.BILLTYPE, 
            strm.BILLTYPEKEY, 
            strm.COMMENTSKEY, 
            strm.CONDITIONFORMULAKEY, 
            strm.CORRPROCESSSETUP, 
            strm.DEFAULTFORREFUND, 
            strm.DELETED, 
            strm.DELINQUENCYEXEMPTFLAG, 
            strm.DELINQUENCYTYPE, 
            strm.DELINQUENTDATEDAYS, 
            strm.DELINQUENTDATEFORMULAKEY, 
            strm.DISCNTPENDAYFORMULAKEY, 
            strm.DISCOUNTPENALTYDAYS, 
            strm.DISCOUNTPENALTYLISUKEY, 
            strm.DISCOUNTPENALTYTYPE, 
            strm.DISCOUNTPENALTYVARIANCE, 
            strm.DUEDATEDAYS, 
            strm.DUEDATEFORMULAKEY, 
            strm.DYNAMICBPFLAG, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.INCLUDESERVICEDATES, 
            strm.INTERIMTYPEFLAG, 
            strm.MINCHARGEAMOUNT, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PAYDEPOSITFIRSTFLAG, 
            strm.PAYMENTAPPLICATIONRULE, 
            strm.PAYMENTORDER, 
            strm.PENALIZEENTIREBILLFLAG, 
            strm.PENALLFLAG, 
            strm.PENALTYAGEDFLAG, 
            strm.PENALTYCODEKEY, 
            strm.PENALTYPENFLAG, 
            strm.READINGSTRACKED, 
            strm.USEWINTERAVERAGEUSAGEONLYFLAG, 
            strm.VARIATION_ID, 
            strm.VIEWBILLIMAGEFORMULA, 
            strm.WAIVEPENALTYDEFAULTCODE, 
            strm.WAIVEPENALTYLOG, 
            strm.WHERECLAUSE, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.BILLTYPEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_BILLTYPE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.BILLTYPEKEY=src.BILLTYPEKEY) OR (target.BILLTYPEKEY IS NULL AND src.BILLTYPEKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPLYCREDITTYPE=src.APPLYCREDITTYPE, 
                    target.BILLDESC=src.BILLDESC, 
                    target.BILLINGFAMILY=src.BILLINGFAMILY, 
                    target.BILLMAXIMUM=src.BILLMAXIMUM, 
                    target.BILLMINIMUM=src.BILLMINIMUM, 
                    target.BILLTYPE=src.BILLTYPE, 
                    target.BILLTYPEKEY=src.BILLTYPEKEY, 
                    target.COMMENTSKEY=src.COMMENTSKEY, 
                    target.CONDITIONFORMULAKEY=src.CONDITIONFORMULAKEY, 
                    target.CORRPROCESSSETUP=src.CORRPROCESSSETUP, 
                    target.DEFAULTFORREFUND=src.DEFAULTFORREFUND, 
                    target.DELETED=src.DELETED, 
                    target.DELINQUENCYEXEMPTFLAG=src.DELINQUENCYEXEMPTFLAG, 
                    target.DELINQUENCYTYPE=src.DELINQUENCYTYPE, 
                    target.DELINQUENTDATEDAYS=src.DELINQUENTDATEDAYS, 
                    target.DELINQUENTDATEFORMULAKEY=src.DELINQUENTDATEFORMULAKEY, 
                    target.DISCNTPENDAYFORMULAKEY=src.DISCNTPENDAYFORMULAKEY, 
                    target.DISCOUNTPENALTYDAYS=src.DISCOUNTPENALTYDAYS, 
                    target.DISCOUNTPENALTYLISUKEY=src.DISCOUNTPENALTYLISUKEY, 
                    target.DISCOUNTPENALTYTYPE=src.DISCOUNTPENALTYTYPE, 
                    target.DISCOUNTPENALTYVARIANCE=src.DISCOUNTPENALTYVARIANCE, 
                    target.DUEDATEDAYS=src.DUEDATEDAYS, 
                    target.DUEDATEFORMULAKEY=src.DUEDATEFORMULAKEY, 
                    target.DYNAMICBPFLAG=src.DYNAMICBPFLAG, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.INCLUDESERVICEDATES=src.INCLUDESERVICEDATES, 
                    target.INTERIMTYPEFLAG=src.INTERIMTYPEFLAG, 
                    target.MINCHARGEAMOUNT=src.MINCHARGEAMOUNT, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PAYDEPOSITFIRSTFLAG=src.PAYDEPOSITFIRSTFLAG, 
                    target.PAYMENTAPPLICATIONRULE=src.PAYMENTAPPLICATIONRULE, 
                    target.PAYMENTORDER=src.PAYMENTORDER, 
                    target.PENALIZEENTIREBILLFLAG=src.PENALIZEENTIREBILLFLAG, 
                    target.PENALLFLAG=src.PENALLFLAG, 
                    target.PENALTYAGEDFLAG=src.PENALTYAGEDFLAG, 
                    target.PENALTYCODEKEY=src.PENALTYCODEKEY, 
                    target.PENALTYPENFLAG=src.PENALTYPENFLAG, 
                    target.READINGSTRACKED=src.READINGSTRACKED, 
                    target.USEWINTERAVERAGEUSAGEONLYFLAG=src.USEWINTERAVERAGEUSAGEONLYFLAG, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VIEWBILLIMAGEFORMULA=src.VIEWBILLIMAGEFORMULA, 
                    target.WAIVEPENALTYDEFAULTCODE=src.WAIVEPENALTYDEFAULTCODE, 
                    target.WAIVEPENALTYLOG=src.WAIVEPENALTYLOG, 
                    target.WHERECLAUSE=src.WHERECLAUSE, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    APPLYCREDITTYPE, 
                    BILLDESC, 
                    BILLINGFAMILY, 
                    BILLMAXIMUM, 
                    BILLMINIMUM, 
                    BILLTYPE, 
                    BILLTYPEKEY, 
                    COMMENTSKEY, 
                    CONDITIONFORMULAKEY, 
                    CORRPROCESSSETUP, 
                    DEFAULTFORREFUND, 
                    DELETED, 
                    DELINQUENCYEXEMPTFLAG, 
                    DELINQUENCYTYPE, 
                    DELINQUENTDATEDAYS, 
                    DELINQUENTDATEFORMULAKEY, 
                    DISCNTPENDAYFORMULAKEY, 
                    DISCOUNTPENALTYDAYS, 
                    DISCOUNTPENALTYLISUKEY, 
                    DISCOUNTPENALTYTYPE, 
                    DISCOUNTPENALTYVARIANCE, 
                    DUEDATEDAYS, 
                    DUEDATEFORMULAKEY, 
                    DYNAMICBPFLAG, 
                    EFFDATE, 
                    EXPDATE, 
                    INCLUDESERVICEDATES, 
                    INTERIMTYPEFLAG, 
                    MINCHARGEAMOUNT, 
                    MODBY, 
                    MODDTTM, 
                    PAYDEPOSITFIRSTFLAG, 
                    PAYMENTAPPLICATIONRULE, 
                    PAYMENTORDER, 
                    PENALIZEENTIREBILLFLAG, 
                    PENALLFLAG, 
                    PENALTYAGEDFLAG, 
                    PENALTYCODEKEY, 
                    PENALTYPENFLAG, 
                    READINGSTRACKED, 
                    USEWINTERAVERAGEUSAGEONLYFLAG, 
                    VARIATION_ID, 
                    VIEWBILLIMAGEFORMULA, 
                    WAIVEPENALTYDEFAULTCODE, 
                    WAIVEPENALTYLOG, 
                    WHERECLAUSE, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPLYCREDITTYPE, 
                    src.BILLDESC, 
                    src.BILLINGFAMILY, 
                    src.BILLMAXIMUM, 
                    src.BILLMINIMUM, 
                    src.BILLTYPE, 
                    src.BILLTYPEKEY, 
                    src.COMMENTSKEY, 
                    src.CONDITIONFORMULAKEY, 
                    src.CORRPROCESSSETUP, 
                    src.DEFAULTFORREFUND, 
                    src.DELETED, 
                    src.DELINQUENCYEXEMPTFLAG, 
                    src.DELINQUENCYTYPE, 
                    src.DELINQUENTDATEDAYS, 
                    src.DELINQUENTDATEFORMULAKEY, 
                    src.DISCNTPENDAYFORMULAKEY, 
                    src.DISCOUNTPENALTYDAYS, 
                    src.DISCOUNTPENALTYLISUKEY, 
                    src.DISCOUNTPENALTYTYPE, 
                    src.DISCOUNTPENALTYVARIANCE, 
                    src.DUEDATEDAYS, 
                    src.DUEDATEFORMULAKEY, 
                    src.DYNAMICBPFLAG, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.INCLUDESERVICEDATES, 
                    src.INTERIMTYPEFLAG, 
                    src.MINCHARGEAMOUNT, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PAYDEPOSITFIRSTFLAG, 
                    src.PAYMENTAPPLICATIONRULE, 
                    src.PAYMENTORDER, 
                    src.PENALIZEENTIREBILLFLAG, 
                    src.PENALLFLAG, 
                    src.PENALTYAGEDFLAG, 
                    src.PENALTYCODEKEY, 
                    src.PENALTYPENFLAG, 
                    src.READINGSTRACKED, 
                    src.USEWINTERAVERAGEUSAGEONLYFLAG, 
                    src.VARIATION_ID, 
                    src.VIEWBILLIMAGEFORMULA, 
                    src.WAIVEPENALTYDEFAULTCODE, 
                    src.WAIVEPENALTYLOG, 
                    src.WHERECLAUSE,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_BILLTYPE as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPLYCREDITTYPE, 
            strm.BILLDESC, 
            strm.BILLINGFAMILY, 
            strm.BILLMAXIMUM, 
            strm.BILLMINIMUM, 
            strm.BILLTYPE, 
            strm.BILLTYPEKEY, 
            strm.COMMENTSKEY, 
            strm.CONDITIONFORMULAKEY, 
            strm.CORRPROCESSSETUP, 
            strm.DEFAULTFORREFUND, 
            strm.DELETED, 
            strm.DELINQUENCYEXEMPTFLAG, 
            strm.DELINQUENCYTYPE, 
            strm.DELINQUENTDATEDAYS, 
            strm.DELINQUENTDATEFORMULAKEY, 
            strm.DISCNTPENDAYFORMULAKEY, 
            strm.DISCOUNTPENALTYDAYS, 
            strm.DISCOUNTPENALTYLISUKEY, 
            strm.DISCOUNTPENALTYTYPE, 
            strm.DISCOUNTPENALTYVARIANCE, 
            strm.DUEDATEDAYS, 
            strm.DUEDATEFORMULAKEY, 
            strm.DYNAMICBPFLAG, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.INCLUDESERVICEDATES, 
            strm.INTERIMTYPEFLAG, 
            strm.MINCHARGEAMOUNT, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PAYDEPOSITFIRSTFLAG, 
            strm.PAYMENTAPPLICATIONRULE, 
            strm.PAYMENTORDER, 
            strm.PENALIZEENTIREBILLFLAG, 
            strm.PENALLFLAG, 
            strm.PENALTYAGEDFLAG, 
            strm.PENALTYCODEKEY, 
            strm.PENALTYPENFLAG, 
            strm.READINGSTRACKED, 
            strm.USEWINTERAVERAGEUSAGEONLYFLAG, 
            strm.VARIATION_ID, 
            strm.VIEWBILLIMAGEFORMULA, 
            strm.WAIVEPENALTYDEFAULTCODE, 
            strm.WAIVEPENALTYLOG, 
            strm.WHERECLAUSE, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.BILLTYPEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_BILLTYPE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.BILLTYPEKEY=src.BILLTYPEKEY) OR (target.BILLTYPEKEY IS NULL AND src.BILLTYPEKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPLYCREDITTYPE=src.APPLYCREDITTYPE, 
                    target.BILLDESC=src.BILLDESC, 
                    target.BILLINGFAMILY=src.BILLINGFAMILY, 
                    target.BILLMAXIMUM=src.BILLMAXIMUM, 
                    target.BILLMINIMUM=src.BILLMINIMUM, 
                    target.BILLTYPE=src.BILLTYPE, 
                    target.BILLTYPEKEY=src.BILLTYPEKEY, 
                    target.COMMENTSKEY=src.COMMENTSKEY, 
                    target.CONDITIONFORMULAKEY=src.CONDITIONFORMULAKEY, 
                    target.CORRPROCESSSETUP=src.CORRPROCESSSETUP, 
                    target.DEFAULTFORREFUND=src.DEFAULTFORREFUND, 
                    target.DELETED=src.DELETED, 
                    target.DELINQUENCYEXEMPTFLAG=src.DELINQUENCYEXEMPTFLAG, 
                    target.DELINQUENCYTYPE=src.DELINQUENCYTYPE, 
                    target.DELINQUENTDATEDAYS=src.DELINQUENTDATEDAYS, 
                    target.DELINQUENTDATEFORMULAKEY=src.DELINQUENTDATEFORMULAKEY, 
                    target.DISCNTPENDAYFORMULAKEY=src.DISCNTPENDAYFORMULAKEY, 
                    target.DISCOUNTPENALTYDAYS=src.DISCOUNTPENALTYDAYS, 
                    target.DISCOUNTPENALTYLISUKEY=src.DISCOUNTPENALTYLISUKEY, 
                    target.DISCOUNTPENALTYTYPE=src.DISCOUNTPENALTYTYPE, 
                    target.DISCOUNTPENALTYVARIANCE=src.DISCOUNTPENALTYVARIANCE, 
                    target.DUEDATEDAYS=src.DUEDATEDAYS, 
                    target.DUEDATEFORMULAKEY=src.DUEDATEFORMULAKEY, 
                    target.DYNAMICBPFLAG=src.DYNAMICBPFLAG, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.INCLUDESERVICEDATES=src.INCLUDESERVICEDATES, 
                    target.INTERIMTYPEFLAG=src.INTERIMTYPEFLAG, 
                    target.MINCHARGEAMOUNT=src.MINCHARGEAMOUNT, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PAYDEPOSITFIRSTFLAG=src.PAYDEPOSITFIRSTFLAG, 
                    target.PAYMENTAPPLICATIONRULE=src.PAYMENTAPPLICATIONRULE, 
                    target.PAYMENTORDER=src.PAYMENTORDER, 
                    target.PENALIZEENTIREBILLFLAG=src.PENALIZEENTIREBILLFLAG, 
                    target.PENALLFLAG=src.PENALLFLAG, 
                    target.PENALTYAGEDFLAG=src.PENALTYAGEDFLAG, 
                    target.PENALTYCODEKEY=src.PENALTYCODEKEY, 
                    target.PENALTYPENFLAG=src.PENALTYPENFLAG, 
                    target.READINGSTRACKED=src.READINGSTRACKED, 
                    target.USEWINTERAVERAGEUSAGEONLYFLAG=src.USEWINTERAVERAGEUSAGEONLYFLAG, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VIEWBILLIMAGEFORMULA=src.VIEWBILLIMAGEFORMULA, 
                    target.WAIVEPENALTYDEFAULTCODE=src.WAIVEPENALTYDEFAULTCODE, 
                    target.WAIVEPENALTYLOG=src.WAIVEPENALTYLOG, 
                    target.WHERECLAUSE=src.WHERECLAUSE, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, APPLYCREDITTYPE, BILLDESC, BILLINGFAMILY, BILLMAXIMUM, BILLMINIMUM, BILLTYPE, BILLTYPEKEY, COMMENTSKEY, CONDITIONFORMULAKEY, CORRPROCESSSETUP, DEFAULTFORREFUND, DELETED, DELINQUENCYEXEMPTFLAG, DELINQUENCYTYPE, DELINQUENTDATEDAYS, DELINQUENTDATEFORMULAKEY, DISCNTPENDAYFORMULAKEY, DISCOUNTPENALTYDAYS, DISCOUNTPENALTYLISUKEY, DISCOUNTPENALTYTYPE, DISCOUNTPENALTYVARIANCE, DUEDATEDAYS, DUEDATEFORMULAKEY, DYNAMICBPFLAG, EFFDATE, EXPDATE, INCLUDESERVICEDATES, INTERIMTYPEFLAG, MINCHARGEAMOUNT, MODBY, MODDTTM, PAYDEPOSITFIRSTFLAG, PAYMENTAPPLICATIONRULE, PAYMENTORDER, PENALIZEENTIREBILLFLAG, PENALLFLAG, PENALTYAGEDFLAG, PENALTYCODEKEY, PENALTYPENFLAG, READINGSTRACKED, USEWINTERAVERAGEUSAGEONLYFLAG, VARIATION_ID, VIEWBILLIMAGEFORMULA, WAIVEPENALTYDEFAULTCODE, WAIVEPENALTYLOG, WHERECLAUSE, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPLYCREDITTYPE, 
                    src.BILLDESC, 
                    src.BILLINGFAMILY, 
                    src.BILLMAXIMUM, 
                    src.BILLMINIMUM, 
                    src.BILLTYPE, 
                    src.BILLTYPEKEY, 
                    src.COMMENTSKEY, 
                    src.CONDITIONFORMULAKEY, 
                    src.CORRPROCESSSETUP, 
                    src.DEFAULTFORREFUND, 
                    src.DELETED, 
                    src.DELINQUENCYEXEMPTFLAG, 
                    src.DELINQUENCYTYPE, 
                    src.DELINQUENTDATEDAYS, 
                    src.DELINQUENTDATEFORMULAKEY, 
                    src.DISCNTPENDAYFORMULAKEY, 
                    src.DISCOUNTPENALTYDAYS, 
                    src.DISCOUNTPENALTYLISUKEY, 
                    src.DISCOUNTPENALTYTYPE, 
                    src.DISCOUNTPENALTYVARIANCE, 
                    src.DUEDATEDAYS, 
                    src.DUEDATEFORMULAKEY, 
                    src.DYNAMICBPFLAG, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.INCLUDESERVICEDATES, 
                    src.INTERIMTYPEFLAG, 
                    src.MINCHARGEAMOUNT, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PAYDEPOSITFIRSTFLAG, 
                    src.PAYMENTAPPLICATIONRULE, 
                    src.PAYMENTORDER, 
                    src.PENALIZEENTIREBILLFLAG, 
                    src.PENALLFLAG, 
                    src.PENALTYAGEDFLAG, 
                    src.PENALTYCODEKEY, 
                    src.PENALTYPENFLAG, 
                    src.READINGSTRACKED, 
                    src.USEWINTERAVERAGEUSAGEONLYFLAG, 
                    src.VARIATION_ID, 
                    src.VIEWBILLIMAGEFORMULA, 
                    src.WAIVEPENALTYDEFAULTCODE, 
                    src.WAIVEPENALTYLOG, 
                    src.WHERECLAUSE,     
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
create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_LINEITEM()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_LINEITEM_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_LINEITEM as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTSERVICEKEY, 
            strm.ACTUALAMT, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BILLKEY, 
            strm.BUDGETEDAMOUNTFLAG, 
            strm.COMMENTSKEY, 
            strm.DATALAKE_DELETED, 
            strm.DISCOUNTEXPIREDATE, 
            strm.LINEITEMAMT, 
            strm.LINEITEMKEY, 
            strm.LINEITEMSETUPKEY, 
            strm.LINEITEMUNITS, 
            strm.LINEITEMUSAGE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PAIDSTAT, 
            strm.PENALIZEDFLAG, 
            strm.PENALTYDATE, 
            strm.PENALTYPAYORDER, 
            strm.PENDTYPE, 
            strm.PRINCIPALPAYORDER, 
            strm.PRINTORDER, 
            strm.PRINTTEXT, 
            strm.VARIANTFLAG, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.LINEITEMKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_LINEITEM as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.LINEITEMKEY=src.LINEITEMKEY) OR (target.LINEITEMKEY IS NULL AND src.LINEITEMKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTSERVICEKEY=src.ACCOUNTSERVICEKEY, 
                    target.ACTUALAMT=src.ACTUALAMT, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BILLKEY=src.BILLKEY, 
                    target.BUDGETEDAMOUNTFLAG=src.BUDGETEDAMOUNTFLAG, 
                    target.COMMENTSKEY=src.COMMENTSKEY, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DISCOUNTEXPIREDATE=src.DISCOUNTEXPIREDATE, 
                    target.LINEITEMAMT=src.LINEITEMAMT, 
                    target.LINEITEMKEY=src.LINEITEMKEY, 
                    target.LINEITEMSETUPKEY=src.LINEITEMSETUPKEY, 
                    target.LINEITEMUNITS=src.LINEITEMUNITS, 
                    target.LINEITEMUSAGE=src.LINEITEMUSAGE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PAIDSTAT=src.PAIDSTAT, 
                    target.PENALIZEDFLAG=src.PENALIZEDFLAG, 
                    target.PENALTYDATE=src.PENALTYDATE, 
                    target.PENALTYPAYORDER=src.PENALTYPAYORDER, 
                    target.PENDTYPE=src.PENDTYPE, 
                    target.PRINCIPALPAYORDER=src.PRINCIPALPAYORDER, 
                    target.PRINTORDER=src.PRINTORDER, 
                    target.PRINTTEXT=src.PRINTTEXT, 
                    target.VARIANTFLAG=src.VARIANTFLAG, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTSERVICEKEY, 
                    ACTUALAMT, 
                    ADDBY, 
                    ADDDTTM, 
                    BILLKEY, 
                    BUDGETEDAMOUNTFLAG, 
                    COMMENTSKEY, 
                    DATALAKE_DELETED, 
                    DISCOUNTEXPIREDATE, 
                    LINEITEMAMT, 
                    LINEITEMKEY, 
                    LINEITEMSETUPKEY, 
                    LINEITEMUNITS, 
                    LINEITEMUSAGE, 
                    MODBY, 
                    MODDTTM, 
                    PAIDSTAT, 
                    PENALIZEDFLAG, 
                    PENALTYDATE, 
                    PENALTYPAYORDER, 
                    PENDTYPE, 
                    PRINCIPALPAYORDER, 
                    PRINTORDER, 
                    PRINTTEXT, 
                    VARIANTFLAG, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTSERVICEKEY, 
                    src.ACTUALAMT, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BILLKEY, 
                    src.BUDGETEDAMOUNTFLAG, 
                    src.COMMENTSKEY, 
                    src.DATALAKE_DELETED, 
                    src.DISCOUNTEXPIREDATE, 
                    src.LINEITEMAMT, 
                    src.LINEITEMKEY, 
                    src.LINEITEMSETUPKEY, 
                    src.LINEITEMUNITS, 
                    src.LINEITEMUSAGE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PAIDSTAT, 
                    src.PENALIZEDFLAG, 
                    src.PENALTYDATE, 
                    src.PENALTYPAYORDER, 
                    src.PENDTYPE, 
                    src.PRINCIPALPAYORDER, 
                    src.PRINTORDER, 
                    src.PRINTTEXT, 
                    src.VARIANTFLAG, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_LINEITEM as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTSERVICEKEY, 
            strm.ACTUALAMT, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BILLKEY, 
            strm.BUDGETEDAMOUNTFLAG, 
            strm.COMMENTSKEY, 
            strm.DATALAKE_DELETED, 
            strm.DISCOUNTEXPIREDATE, 
            strm.LINEITEMAMT, 
            strm.LINEITEMKEY, 
            strm.LINEITEMSETUPKEY, 
            strm.LINEITEMUNITS, 
            strm.LINEITEMUSAGE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PAIDSTAT, 
            strm.PENALIZEDFLAG, 
            strm.PENALTYDATE, 
            strm.PENALTYPAYORDER, 
            strm.PENDTYPE, 
            strm.PRINCIPALPAYORDER, 
            strm.PRINTORDER, 
            strm.PRINTTEXT, 
            strm.VARIANTFLAG, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.LINEITEMKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_LINEITEM as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.LINEITEMKEY=src.LINEITEMKEY) OR (target.LINEITEMKEY IS NULL AND src.LINEITEMKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTSERVICEKEY=src.ACCOUNTSERVICEKEY, 
                    target.ACTUALAMT=src.ACTUALAMT, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BILLKEY=src.BILLKEY, 
                    target.BUDGETEDAMOUNTFLAG=src.BUDGETEDAMOUNTFLAG, 
                    target.COMMENTSKEY=src.COMMENTSKEY, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DISCOUNTEXPIREDATE=src.DISCOUNTEXPIREDATE, 
                    target.LINEITEMAMT=src.LINEITEMAMT, 
                    target.LINEITEMKEY=src.LINEITEMKEY, 
                    target.LINEITEMSETUPKEY=src.LINEITEMSETUPKEY, 
                    target.LINEITEMUNITS=src.LINEITEMUNITS, 
                    target.LINEITEMUSAGE=src.LINEITEMUSAGE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PAIDSTAT=src.PAIDSTAT, 
                    target.PENALIZEDFLAG=src.PENALIZEDFLAG, 
                    target.PENALTYDATE=src.PENALTYDATE, 
                    target.PENALTYPAYORDER=src.PENALTYPAYORDER, 
                    target.PENDTYPE=src.PENDTYPE, 
                    target.PRINCIPALPAYORDER=src.PRINCIPALPAYORDER, 
                    target.PRINTORDER=src.PRINTORDER, 
                    target.PRINTTEXT=src.PRINTTEXT, 
                    target.VARIANTFLAG=src.VARIANTFLAG, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTSERVICEKEY, ACTUALAMT, ADDBY, ADDDTTM, BILLKEY, BUDGETEDAMOUNTFLAG, COMMENTSKEY, DATALAKE_DELETED, DISCOUNTEXPIREDATE, LINEITEMAMT, LINEITEMKEY, LINEITEMSETUPKEY, LINEITEMUNITS, LINEITEMUSAGE, MODBY, MODDTTM, PAIDSTAT, PENALIZEDFLAG, PENALTYDATE, PENALTYPAYORDER, PENDTYPE, PRINCIPALPAYORDER, PRINTORDER, PRINTTEXT, VARIANTFLAG, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTSERVICEKEY, 
                    src.ACTUALAMT, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.BILLKEY, 
                    src.BUDGETEDAMOUNTFLAG, 
                    src.COMMENTSKEY, 
                    src.DATALAKE_DELETED, 
                    src.DISCOUNTEXPIREDATE, 
                    src.LINEITEMAMT, 
                    src.LINEITEMKEY, 
                    src.LINEITEMSETUPKEY, 
                    src.LINEITEMUNITS, 
                    src.LINEITEMUSAGE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PAIDSTAT, 
                    src.PENALIZEDFLAG, 
                    src.PENALTYDATE, 
                    src.PENALTYPAYORDER, 
                    src.PENDTYPE, 
                    src.PRINCIPALPAYORDER, 
                    src.PRINTORDER, 
                    src.PRINTTEXT, 
                    src.VARIANTFLAG, 
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
create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_DELINQUENCYHISTORY()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_DELINQUENCYHISTORY_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_DELINQUENCYHISTORY as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTKEY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDRESSLIENKEY, 
            strm.AUTODECANDRESPAYPOSTDTTM, 
            strm.BILLKEY, 
            strm.BILLTYPEKEY, 
            strm.COLLECTIONSKEY, 
            strm.COLLECTIONSRUNKEY, 
            strm.COMMENTS, 
            strm.DELETED, 
            strm.DELINQUENCYENTRYAMOUNT, 
            strm.DELINQUENCYENTRYBALANCE, 
            strm.DELINQUENCYENTRYDATE, 
            strm.DELINQUENCYHISTORYKEY, 
            strm.DELINQUENCYLEVEL, 
            strm.DELINQUENCYMANAGER, 
            strm.DELINQUENCYOFFICER, 
            strm.DELINQUENCYRUNKEY, 
            strm.DELINQUENTAMOUNT, 
            strm.DELINQUENTTHROUGHDATE, 
            strm.DISPLAYHISTORYRECORD, 
            strm.EXTENSION, 
            strm.HOLDFLAG, 
            strm.MILESTONEDUEDATE, 
            strm.MILESTONEENTRYDATE, 
            strm.MILESTONEENTRYREASON, 
            strm.MILESTONEKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PARCELLIENKEY, 
            strm.PROCESSINGRUNKEY, 
            strm.RESOLUTION, 
            strm.RESOLVEDBY, 
            strm.RESOLVEDDATE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DELINQUENCYHISTORYKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_DELINQUENCYHISTORY as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DELINQUENCYHISTORYKEY=src.DELINQUENCYHISTORYKEY) OR (target.DELINQUENCYHISTORYKEY IS NULL AND src.DELINQUENCYHISTORYKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDRESSLIENKEY=src.ADDRESSLIENKEY, 
                    target.AUTODECANDRESPAYPOSTDTTM=src.AUTODECANDRESPAYPOSTDTTM, 
                    target.BILLKEY=src.BILLKEY, 
                    target.BILLTYPEKEY=src.BILLTYPEKEY, 
                    target.COLLECTIONSKEY=src.COLLECTIONSKEY, 
                    target.COLLECTIONSRUNKEY=src.COLLECTIONSRUNKEY, 
                    target.COMMENTS=src.COMMENTS, 
                    target.DELETED=src.DELETED, 
                    target.DELINQUENCYENTRYAMOUNT=src.DELINQUENCYENTRYAMOUNT, 
                    target.DELINQUENCYENTRYBALANCE=src.DELINQUENCYENTRYBALANCE, 
                    target.DELINQUENCYENTRYDATE=src.DELINQUENCYENTRYDATE, 
                    target.DELINQUENCYHISTORYKEY=src.DELINQUENCYHISTORYKEY, 
                    target.DELINQUENCYLEVEL=src.DELINQUENCYLEVEL, 
                    target.DELINQUENCYMANAGER=src.DELINQUENCYMANAGER, 
                    target.DELINQUENCYOFFICER=src.DELINQUENCYOFFICER, 
                    target.DELINQUENCYRUNKEY=src.DELINQUENCYRUNKEY, 
                    target.DELINQUENTAMOUNT=src.DELINQUENTAMOUNT, 
                    target.DELINQUENTTHROUGHDATE=src.DELINQUENTTHROUGHDATE, 
                    target.DISPLAYHISTORYRECORD=src.DISPLAYHISTORYRECORD, 
                    target.EXTENSION=src.EXTENSION, 
                    target.HOLDFLAG=src.HOLDFLAG, 
                    target.MILESTONEDUEDATE=src.MILESTONEDUEDATE, 
                    target.MILESTONEENTRYDATE=src.MILESTONEENTRYDATE, 
                    target.MILESTONEENTRYREASON=src.MILESTONEENTRYREASON, 
                    target.MILESTONEKEY=src.MILESTONEKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PARCELLIENKEY=src.PARCELLIENKEY, 
                    target.PROCESSINGRUNKEY=src.PROCESSINGRUNKEY, 
                    target.RESOLUTION=src.RESOLUTION, 
                    target.RESOLVEDBY=src.RESOLVEDBY, 
                    target.RESOLVEDDATE=src.RESOLVEDDATE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTKEY, 
                    ADDBY, 
                    ADDDTTM, 
                    ADDRESSLIENKEY, 
                    AUTODECANDRESPAYPOSTDTTM, 
                    BILLKEY, 
                    BILLTYPEKEY, 
                    COLLECTIONSKEY, 
                    COLLECTIONSRUNKEY, 
                    COMMENTS, 
                    DELETED, 
                    DELINQUENCYENTRYAMOUNT, 
                    DELINQUENCYENTRYBALANCE, 
                    DELINQUENCYENTRYDATE, 
                    DELINQUENCYHISTORYKEY, 
                    DELINQUENCYLEVEL, 
                    DELINQUENCYMANAGER, 
                    DELINQUENCYOFFICER, 
                    DELINQUENCYRUNKEY, 
                    DELINQUENTAMOUNT, 
                    DELINQUENTTHROUGHDATE, 
                    DISPLAYHISTORYRECORD, 
                    EXTENSION, 
                    HOLDFLAG, 
                    MILESTONEDUEDATE, 
                    MILESTONEENTRYDATE, 
                    MILESTONEENTRYREASON, 
                    MILESTONEKEY, 
                    MODBY, 
                    MODDTTM, 
                    PARCELLIENKEY, 
                    PROCESSINGRUNKEY, 
                    RESOLUTION, 
                    RESOLVEDBY, 
                    RESOLVEDDATE, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTKEY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDRESSLIENKEY, 
                    src.AUTODECANDRESPAYPOSTDTTM, 
                    src.BILLKEY, 
                    src.BILLTYPEKEY, 
                    src.COLLECTIONSKEY, 
                    src.COLLECTIONSRUNKEY, 
                    src.COMMENTS, 
                    src.DELETED, 
                    src.DELINQUENCYENTRYAMOUNT, 
                    src.DELINQUENCYENTRYBALANCE, 
                    src.DELINQUENCYENTRYDATE, 
                    src.DELINQUENCYHISTORYKEY, 
                    src.DELINQUENCYLEVEL, 
                    src.DELINQUENCYMANAGER, 
                    src.DELINQUENCYOFFICER, 
                    src.DELINQUENCYRUNKEY, 
                    src.DELINQUENTAMOUNT, 
                    src.DELINQUENTTHROUGHDATE, 
                    src.DISPLAYHISTORYRECORD, 
                    src.EXTENSION, 
                    src.HOLDFLAG, 
                    src.MILESTONEDUEDATE, 
                    src.MILESTONEENTRYDATE, 
                    src.MILESTONEENTRYREASON, 
                    src.MILESTONEKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PARCELLIENKEY, 
                    src.PROCESSINGRUNKEY, 
                    src.RESOLUTION, 
                    src.RESOLVEDBY, 
                    src.RESOLVEDDATE, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_DELINQUENCYHISTORY as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTKEY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDRESSLIENKEY, 
            strm.AUTODECANDRESPAYPOSTDTTM, 
            strm.BILLKEY, 
            strm.BILLTYPEKEY, 
            strm.COLLECTIONSKEY, 
            strm.COLLECTIONSRUNKEY, 
            strm.COMMENTS, 
            strm.DELETED, 
            strm.DELINQUENCYENTRYAMOUNT, 
            strm.DELINQUENCYENTRYBALANCE, 
            strm.DELINQUENCYENTRYDATE, 
            strm.DELINQUENCYHISTORYKEY, 
            strm.DELINQUENCYLEVEL, 
            strm.DELINQUENCYMANAGER, 
            strm.DELINQUENCYOFFICER, 
            strm.DELINQUENCYRUNKEY, 
            strm.DELINQUENTAMOUNT, 
            strm.DELINQUENTTHROUGHDATE, 
            strm.DISPLAYHISTORYRECORD, 
            strm.EXTENSION, 
            strm.HOLDFLAG, 
            strm.MILESTONEDUEDATE, 
            strm.MILESTONEENTRYDATE, 
            strm.MILESTONEENTRYREASON, 
            strm.MILESTONEKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PARCELLIENKEY, 
            strm.PROCESSINGRUNKEY, 
            strm.RESOLUTION, 
            strm.RESOLVEDBY, 
            strm.RESOLVEDDATE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.DELINQUENCYHISTORYKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_DELINQUENCYHISTORY as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.DELINQUENCYHISTORYKEY=src.DELINQUENCYHISTORYKEY) OR (target.DELINQUENCYHISTORYKEY IS NULL AND src.DELINQUENCYHISTORYKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTKEY=src.ACCOUNTKEY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDRESSLIENKEY=src.ADDRESSLIENKEY, 
                    target.AUTODECANDRESPAYPOSTDTTM=src.AUTODECANDRESPAYPOSTDTTM, 
                    target.BILLKEY=src.BILLKEY, 
                    target.BILLTYPEKEY=src.BILLTYPEKEY, 
                    target.COLLECTIONSKEY=src.COLLECTIONSKEY, 
                    target.COLLECTIONSRUNKEY=src.COLLECTIONSRUNKEY, 
                    target.COMMENTS=src.COMMENTS, 
                    target.DELETED=src.DELETED, 
                    target.DELINQUENCYENTRYAMOUNT=src.DELINQUENCYENTRYAMOUNT, 
                    target.DELINQUENCYENTRYBALANCE=src.DELINQUENCYENTRYBALANCE, 
                    target.DELINQUENCYENTRYDATE=src.DELINQUENCYENTRYDATE, 
                    target.DELINQUENCYHISTORYKEY=src.DELINQUENCYHISTORYKEY, 
                    target.DELINQUENCYLEVEL=src.DELINQUENCYLEVEL, 
                    target.DELINQUENCYMANAGER=src.DELINQUENCYMANAGER, 
                    target.DELINQUENCYOFFICER=src.DELINQUENCYOFFICER, 
                    target.DELINQUENCYRUNKEY=src.DELINQUENCYRUNKEY, 
                    target.DELINQUENTAMOUNT=src.DELINQUENTAMOUNT, 
                    target.DELINQUENTTHROUGHDATE=src.DELINQUENTTHROUGHDATE, 
                    target.DISPLAYHISTORYRECORD=src.DISPLAYHISTORYRECORD, 
                    target.EXTENSION=src.EXTENSION, 
                    target.HOLDFLAG=src.HOLDFLAG, 
                    target.MILESTONEDUEDATE=src.MILESTONEDUEDATE, 
                    target.MILESTONEENTRYDATE=src.MILESTONEENTRYDATE, 
                    target.MILESTONEENTRYREASON=src.MILESTONEENTRYREASON, 
                    target.MILESTONEKEY=src.MILESTONEKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PARCELLIENKEY=src.PARCELLIENKEY, 
                    target.PROCESSINGRUNKEY=src.PROCESSINGRUNKEY, 
                    target.RESOLUTION=src.RESOLUTION, 
                    target.RESOLVEDBY=src.RESOLVEDBY, 
                    target.RESOLVEDDATE=src.RESOLVEDDATE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTKEY, ADDBY, ADDDTTM, ADDRESSLIENKEY, AUTODECANDRESPAYPOSTDTTM, BILLKEY, BILLTYPEKEY, COLLECTIONSKEY, COLLECTIONSRUNKEY, COMMENTS, DELETED, DELINQUENCYENTRYAMOUNT, DELINQUENCYENTRYBALANCE, DELINQUENCYENTRYDATE, DELINQUENCYHISTORYKEY, DELINQUENCYLEVEL, DELINQUENCYMANAGER, DELINQUENCYOFFICER, DELINQUENCYRUNKEY, DELINQUENTAMOUNT, DELINQUENTTHROUGHDATE, DISPLAYHISTORYRECORD, EXTENSION, HOLDFLAG, MILESTONEDUEDATE, MILESTONEENTRYDATE, MILESTONEENTRYREASON, MILESTONEKEY, MODBY, MODDTTM, PARCELLIENKEY, PROCESSINGRUNKEY, RESOLUTION, RESOLVEDBY, RESOLVEDDATE, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTKEY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDRESSLIENKEY, 
                    src.AUTODECANDRESPAYPOSTDTTM, 
                    src.BILLKEY, 
                    src.BILLTYPEKEY, 
                    src.COLLECTIONSKEY, 
                    src.COLLECTIONSRUNKEY, 
                    src.COMMENTS, 
                    src.DELETED, 
                    src.DELINQUENCYENTRYAMOUNT, 
                    src.DELINQUENCYENTRYBALANCE, 
                    src.DELINQUENCYENTRYDATE, 
                    src.DELINQUENCYHISTORYKEY, 
                    src.DELINQUENCYLEVEL, 
                    src.DELINQUENCYMANAGER, 
                    src.DELINQUENCYOFFICER, 
                    src.DELINQUENCYRUNKEY, 
                    src.DELINQUENTAMOUNT, 
                    src.DELINQUENTTHROUGHDATE, 
                    src.DISPLAYHISTORYRECORD, 
                    src.EXTENSION, 
                    src.HOLDFLAG, 
                    src.MILESTONEDUEDATE, 
                    src.MILESTONEENTRYDATE, 
                    src.MILESTONEENTRYREASON, 
                    src.MILESTONEKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PARCELLIENKEY, 
                    src.PROCESSINGRUNKEY, 
                    src.RESOLUTION, 
                    src.RESOLVEDBY, 
                    src.RESOLVEDDATE, 
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
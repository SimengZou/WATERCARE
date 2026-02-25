create or replace procedure DATAHUB_INTEGRATION.SP_IPS_CDR_USE_USEINSP()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_CDR_USE_USEINSP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_CDR_USE_USEINSP as target using (SELECT * FROM (SELECT 
            strm.ACTVD, 
            strm.ACTVDBY, 
            strm.ACTVDDTTM, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APUSEINSPKEY, 
            strm.APUSEINSPTYPEKEY, 
            strm.APUSEKEY, 
            strm.ASSIGNED, 
            strm.ASSIGNTO, 
            strm.ASSIGNTOPROVIDER, 
            strm.CALLBY, 
            strm.CALLDTTM, 
            strm.CMPTRGEN, 
            strm.CNTCTINFO, 
            strm.CNTCTPERSON, 
            strm.COMPDTTM, 
            strm.DELETED, 
            strm.INSPBY, 
            strm.INSPBYPROVIDER, 
            strm.INSPGEN, 
            strm.INSPHOURS, 
            strm.INSTRIPKEY, 
            strm.ISPARTIAL, 
            strm.LOC, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.ODOMSTART, 
            strm.ODOMSTOP, 
            strm.ORD, 
            strm.PREFERENCE, 
            strm.RELEVANTINSP, 
            strm.REQUESTEDBY, 
            strm.RESULT, 
            strm.RESULTBY, 
            strm.RESULTBYPROVIDER, 
            strm.RESULTDTTM, 
            strm.SCHED, 
            strm.SCHEDBY, 
            strm.SCHEDDTTM, 
            strm.SPECIALINSTR, 
            strm.STARTDTTM, 
            strm.STAT, 
            strm.TYPENO, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APUSEINSPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_USE_USEINSP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APUSEINSPKEY=src.APUSEINSPKEY) OR (target.APUSEINSPKEY IS NULL AND src.APUSEINSPKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACTVD=src.ACTVD, 
                    target.ACTVDBY=src.ACTVDBY, 
                    target.ACTVDDTTM=src.ACTVDDTTM, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APUSEINSPKEY=src.APUSEINSPKEY, 
                    target.APUSEINSPTYPEKEY=src.APUSEINSPTYPEKEY, 
                    target.APUSEKEY=src.APUSEKEY, 
                    target.ASSIGNED=src.ASSIGNED, 
                    target.ASSIGNTO=src.ASSIGNTO, 
                    target.ASSIGNTOPROVIDER=src.ASSIGNTOPROVIDER, 
                    target.CALLBY=src.CALLBY, 
                    target.CALLDTTM=src.CALLDTTM, 
                    target.CMPTRGEN=src.CMPTRGEN, 
                    target.CNTCTINFO=src.CNTCTINFO, 
                    target.CNTCTPERSON=src.CNTCTPERSON, 
                    target.COMPDTTM=src.COMPDTTM, 
                    target.DELETED=src.DELETED, 
                    target.INSPBY=src.INSPBY, 
                    target.INSPBYPROVIDER=src.INSPBYPROVIDER, 
                    target.INSPGEN=src.INSPGEN, 
                    target.INSPHOURS=src.INSPHOURS, 
                    target.INSTRIPKEY=src.INSTRIPKEY, 
                    target.ISPARTIAL=src.ISPARTIAL, 
                    target.LOC=src.LOC, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.ODOMSTART=src.ODOMSTART, 
                    target.ODOMSTOP=src.ODOMSTOP, 
                    target.ORD=src.ORD, 
                    target.PREFERENCE=src.PREFERENCE, 
                    target.RELEVANTINSP=src.RELEVANTINSP, 
                    target.REQUESTEDBY=src.REQUESTEDBY, 
                    target.RESULT=src.RESULT, 
                    target.RESULTBY=src.RESULTBY, 
                    target.RESULTBYPROVIDER=src.RESULTBYPROVIDER, 
                    target.RESULTDTTM=src.RESULTDTTM, 
                    target.SCHED=src.SCHED, 
                    target.SCHEDBY=src.SCHEDBY, 
                    target.SCHEDDTTM=src.SCHEDDTTM, 
                    target.SPECIALINSTR=src.SPECIALINSTR, 
                    target.STARTDTTM=src.STARTDTTM, 
                    target.STAT=src.STAT, 
                    target.TYPENO=src.TYPENO, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACTVD, 
                    ACTVDBY, 
                    ACTVDDTTM, 
                    ADDBY, 
                    ADDDTTM, 
                    APUSEINSPKEY, 
                    APUSEINSPTYPEKEY, 
                    APUSEKEY, 
                    ASSIGNED, 
                    ASSIGNTO, 
                    ASSIGNTOPROVIDER, 
                    CALLBY, 
                    CALLDTTM, 
                    CMPTRGEN, 
                    CNTCTINFO, 
                    CNTCTPERSON, 
                    COMPDTTM, 
                    DELETED, 
                    INSPBY, 
                    INSPBYPROVIDER, 
                    INSPGEN, 
                    INSPHOURS, 
                    INSTRIPKEY, 
                    ISPARTIAL, 
                    LOC, 
                    MODBY, 
                    MODDTTM, 
                    ODOMSTART, 
                    ODOMSTOP, 
                    ORD, 
                    PREFERENCE, 
                    RELEVANTINSP, 
                    REQUESTEDBY, 
                    RESULT, 
                    RESULTBY, 
                    RESULTBYPROVIDER, 
                    RESULTDTTM, 
                    SCHED, 
                    SCHEDBY, 
                    SCHEDDTTM, 
                    SPECIALINSTR, 
                    STARTDTTM, 
                    STAT, 
                    TYPENO, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACTVD, 
                    src.ACTVDBY, 
                    src.ACTVDDTTM, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APUSEINSPKEY, 
                    src.APUSEINSPTYPEKEY, 
                    src.APUSEKEY, 
                    src.ASSIGNED, 
                    src.ASSIGNTO, 
                    src.ASSIGNTOPROVIDER, 
                    src.CALLBY, 
                    src.CALLDTTM, 
                    src.CMPTRGEN, 
                    src.CNTCTINFO, 
                    src.CNTCTPERSON, 
                    src.COMPDTTM, 
                    src.DELETED, 
                    src.INSPBY, 
                    src.INSPBYPROVIDER, 
                    src.INSPGEN, 
                    src.INSPHOURS, 
                    src.INSTRIPKEY, 
                    src.ISPARTIAL, 
                    src.LOC, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.ODOMSTART, 
                    src.ODOMSTOP, 
                    src.ORD, 
                    src.PREFERENCE, 
                    src.RELEVANTINSP, 
                    src.REQUESTEDBY, 
                    src.RESULT, 
                    src.RESULTBY, 
                    src.RESULTBYPROVIDER, 
                    src.RESULTDTTM, 
                    src.SCHED, 
                    src.SCHEDBY, 
                    src.SCHEDDTTM, 
                    src.SPECIALINSTR, 
                    src.STARTDTTM, 
                    src.STAT, 
                    src.TYPENO, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_USE_USEINSP as target using (
                SELECT * FROM (SELECT 
            strm.ACTVD, 
            strm.ACTVDBY, 
            strm.ACTVDDTTM, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APUSEINSPKEY, 
            strm.APUSEINSPTYPEKEY, 
            strm.APUSEKEY, 
            strm.ASSIGNED, 
            strm.ASSIGNTO, 
            strm.ASSIGNTOPROVIDER, 
            strm.CALLBY, 
            strm.CALLDTTM, 
            strm.CMPTRGEN, 
            strm.CNTCTINFO, 
            strm.CNTCTPERSON, 
            strm.COMPDTTM, 
            strm.DELETED, 
            strm.INSPBY, 
            strm.INSPBYPROVIDER, 
            strm.INSPGEN, 
            strm.INSPHOURS, 
            strm.INSTRIPKEY, 
            strm.ISPARTIAL, 
            strm.LOC, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.ODOMSTART, 
            strm.ODOMSTOP, 
            strm.ORD, 
            strm.PREFERENCE, 
            strm.RELEVANTINSP, 
            strm.REQUESTEDBY, 
            strm.RESULT, 
            strm.RESULTBY, 
            strm.RESULTBYPROVIDER, 
            strm.RESULTDTTM, 
            strm.SCHED, 
            strm.SCHEDBY, 
            strm.SCHEDDTTM, 
            strm.SPECIALINSTR, 
            strm.STARTDTTM, 
            strm.STAT, 
            strm.TYPENO, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APUSEINSPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_USE_USEINSP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APUSEINSPKEY=src.APUSEINSPKEY) OR (target.APUSEINSPKEY IS NULL AND src.APUSEINSPKEY IS NULL)) 
                when matched then update set
                    target.ACTVD=src.ACTVD, 
                    target.ACTVDBY=src.ACTVDBY, 
                    target.ACTVDDTTM=src.ACTVDDTTM, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APUSEINSPKEY=src.APUSEINSPKEY, 
                    target.APUSEINSPTYPEKEY=src.APUSEINSPTYPEKEY, 
                    target.APUSEKEY=src.APUSEKEY, 
                    target.ASSIGNED=src.ASSIGNED, 
                    target.ASSIGNTO=src.ASSIGNTO, 
                    target.ASSIGNTOPROVIDER=src.ASSIGNTOPROVIDER, 
                    target.CALLBY=src.CALLBY, 
                    target.CALLDTTM=src.CALLDTTM, 
                    target.CMPTRGEN=src.CMPTRGEN, 
                    target.CNTCTINFO=src.CNTCTINFO, 
                    target.CNTCTPERSON=src.CNTCTPERSON, 
                    target.COMPDTTM=src.COMPDTTM, 
                    target.DELETED=src.DELETED, 
                    target.INSPBY=src.INSPBY, 
                    target.INSPBYPROVIDER=src.INSPBYPROVIDER, 
                    target.INSPGEN=src.INSPGEN, 
                    target.INSPHOURS=src.INSPHOURS, 
                    target.INSTRIPKEY=src.INSTRIPKEY, 
                    target.ISPARTIAL=src.ISPARTIAL, 
                    target.LOC=src.LOC, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.ODOMSTART=src.ODOMSTART, 
                    target.ODOMSTOP=src.ODOMSTOP, 
                    target.ORD=src.ORD, 
                    target.PREFERENCE=src.PREFERENCE, 
                    target.RELEVANTINSP=src.RELEVANTINSP, 
                    target.REQUESTEDBY=src.REQUESTEDBY, 
                    target.RESULT=src.RESULT, 
                    target.RESULTBY=src.RESULTBY, 
                    target.RESULTBYPROVIDER=src.RESULTBYPROVIDER, 
                    target.RESULTDTTM=src.RESULTDTTM, 
                    target.SCHED=src.SCHED, 
                    target.SCHEDBY=src.SCHEDBY, 
                    target.SCHEDDTTM=src.SCHEDDTTM, 
                    target.SPECIALINSTR=src.SPECIALINSTR, 
                    target.STARTDTTM=src.STARTDTTM, 
                    target.STAT=src.STAT, 
                    target.TYPENO=src.TYPENO, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACTVD, ACTVDBY, ACTVDDTTM, ADDBY, ADDDTTM, APUSEINSPKEY, APUSEINSPTYPEKEY, APUSEKEY, ASSIGNED, ASSIGNTO, ASSIGNTOPROVIDER, CALLBY, CALLDTTM, CMPTRGEN, CNTCTINFO, CNTCTPERSON, COMPDTTM, DELETED, INSPBY, INSPBYPROVIDER, INSPGEN, INSPHOURS, INSTRIPKEY, ISPARTIAL, LOC, MODBY, MODDTTM, ODOMSTART, ODOMSTOP, ORD, PREFERENCE, RELEVANTINSP, REQUESTEDBY, RESULT, RESULTBY, RESULTBYPROVIDER, RESULTDTTM, SCHED, SCHEDBY, SCHEDDTTM, SPECIALINSTR, STARTDTTM, STAT, TYPENO, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACTVD, 
                    src.ACTVDBY, 
                    src.ACTVDDTTM, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APUSEINSPKEY, 
                    src.APUSEINSPTYPEKEY, 
                    src.APUSEKEY, 
                    src.ASSIGNED, 
                    src.ASSIGNTO, 
                    src.ASSIGNTOPROVIDER, 
                    src.CALLBY, 
                    src.CALLDTTM, 
                    src.CMPTRGEN, 
                    src.CNTCTINFO, 
                    src.CNTCTPERSON, 
                    src.COMPDTTM, 
                    src.DELETED, 
                    src.INSPBY, 
                    src.INSPBYPROVIDER, 
                    src.INSPGEN, 
                    src.INSPHOURS, 
                    src.INSTRIPKEY, 
                    src.ISPARTIAL, 
                    src.LOC, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.ODOMSTART, 
                    src.ODOMSTOP, 
                    src.ORD, 
                    src.PREFERENCE, 
                    src.RELEVANTINSP, 
                    src.REQUESTEDBY, 
                    src.RESULT, 
                    src.RESULTBY, 
                    src.RESULTBYPROVIDER, 
                    src.RESULTDTTM, 
                    src.SCHED, 
                    src.SCHEDBY, 
                    src.SCHEDDTTM, 
                    src.SPECIALINSTR, 
                    src.STARTDTTM, 
                    src.STAT, 
                    src.TYPENO, 
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
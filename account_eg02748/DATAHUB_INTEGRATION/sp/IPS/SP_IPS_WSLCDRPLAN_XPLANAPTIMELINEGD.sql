create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLCDRPLAN_XPLANAPTIMELINEGD()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLCDRPLAN_XPLANAPTIMELINEGD_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLCDRPLAN_XPLANAPTIMELINEGD as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AREAHECTARES, 
            strm.COMMENTS, 
            strm.COMPLETEMONTH, 
            strm.COMPLETEYEAR1, 
            strm.DELETED, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NOOFUNITS, 
            strm.STAGE, 
            strm.STARTMONTH, 
            strm.STARTYEAR1, 
            strm.STATUS, 
            strm.VARIATION_ID, 
            strm.XPLANAPTIMELINEDPKEY, 
            strm.XPLANAPTIMELINEGDKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XPLANAPTIMELINEGDKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRPLAN_XPLANAPTIMELINEGD as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XPLANAPTIMELINEGDKEY=src.XPLANAPTIMELINEGDKEY) OR (target.XPLANAPTIMELINEGDKEY IS NULL AND src.XPLANAPTIMELINEGDKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AREAHECTARES=src.AREAHECTARES, 
                    target.COMMENTS=src.COMMENTS, 
                    target.COMPLETEMONTH=src.COMPLETEMONTH, 
                    target.COMPLETEYEAR1=src.COMPLETEYEAR1, 
                    target.DELETED=src.DELETED, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NOOFUNITS=src.NOOFUNITS, 
                    target.STAGE=src.STAGE, 
                    target.STARTMONTH=src.STARTMONTH, 
                    target.STARTYEAR1=src.STARTYEAR1, 
                    target.STATUS=src.STATUS, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.XPLANAPTIMELINEDPKEY=src.XPLANAPTIMELINEDPKEY, 
                    target.XPLANAPTIMELINEGDKEY=src.XPLANAPTIMELINEGDKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    AREAHECTARES, 
                    COMMENTS, 
                    COMPLETEMONTH, 
                    COMPLETEYEAR1, 
                    DELETED, 
                    MODBY, 
                    MODDTTM, 
                    NOOFUNITS, 
                    STAGE, 
                    STARTMONTH, 
                    STARTYEAR1, 
                    STATUS, 
                    VARIATION_ID, 
                    XPLANAPTIMELINEDPKEY, 
                    XPLANAPTIMELINEGDKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AREAHECTARES, 
                    src.COMMENTS, 
                    src.COMPLETEMONTH, 
                    src.COMPLETEYEAR1, 
                    src.DELETED, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NOOFUNITS, 
                    src.STAGE, 
                    src.STARTMONTH, 
                    src.STARTYEAR1, 
                    src.STATUS, 
                    src.VARIATION_ID, 
                    src.XPLANAPTIMELINEDPKEY, 
                    src.XPLANAPTIMELINEGDKEY,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRPLAN_XPLANAPTIMELINEGD as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AREAHECTARES, 
            strm.COMMENTS, 
            strm.COMPLETEMONTH, 
            strm.COMPLETEYEAR1, 
            strm.DELETED, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NOOFUNITS, 
            strm.STAGE, 
            strm.STARTMONTH, 
            strm.STARTYEAR1, 
            strm.STATUS, 
            strm.VARIATION_ID, 
            strm.XPLANAPTIMELINEDPKEY, 
            strm.XPLANAPTIMELINEGDKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XPLANAPTIMELINEGDKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRPLAN_XPLANAPTIMELINEGD as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XPLANAPTIMELINEGDKEY=src.XPLANAPTIMELINEGDKEY) OR (target.XPLANAPTIMELINEGDKEY IS NULL AND src.XPLANAPTIMELINEGDKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AREAHECTARES=src.AREAHECTARES, 
                    target.COMMENTS=src.COMMENTS, 
                    target.COMPLETEMONTH=src.COMPLETEMONTH, 
                    target.COMPLETEYEAR1=src.COMPLETEYEAR1, 
                    target.DELETED=src.DELETED, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NOOFUNITS=src.NOOFUNITS, 
                    target.STAGE=src.STAGE, 
                    target.STARTMONTH=src.STARTMONTH, 
                    target.STARTYEAR1=src.STARTYEAR1, 
                    target.STATUS=src.STATUS, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.XPLANAPTIMELINEDPKEY=src.XPLANAPTIMELINEDPKEY, 
                    target.XPLANAPTIMELINEGDKEY=src.XPLANAPTIMELINEGDKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, AREAHECTARES, COMMENTS, COMPLETEMONTH, COMPLETEYEAR1, DELETED, MODBY, MODDTTM, NOOFUNITS, STAGE, STARTMONTH, STARTYEAR1, STATUS, VARIATION_ID, XPLANAPTIMELINEDPKEY, XPLANAPTIMELINEGDKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AREAHECTARES, 
                    src.COMMENTS, 
                    src.COMPLETEMONTH, 
                    src.COMPLETEYEAR1, 
                    src.DELETED, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NOOFUNITS, 
                    src.STAGE, 
                    src.STARTMONTH, 
                    src.STARTYEAR1, 
                    src.STATUS, 
                    src.VARIATION_ID, 
                    src.XPLANAPTIMELINEDPKEY, 
                    src.XPLANAPTIMELINEGDKEY,     
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
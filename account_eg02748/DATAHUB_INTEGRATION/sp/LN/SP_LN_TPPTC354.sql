create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPTC354()
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
                            INSERT INTO LANDING_ERROR.LN_RAW_ERROR
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPTC354_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPTC354 as target using (SELECT * FROM (SELECT 
            strm.amch_1, 
            strm.amch_2, 
            strm.amch_3, 
            strm.amch_4, 
            strm.ames_1, 
            strm.ames_2, 
            strm.ames_3, 
            strm.ames_4, 
            strm.ccal, 
            strm.ccic, 
            strm.compnr, 
            strm.cprj, 
            strm.cprj_ccal_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.deleted, 
            strm.quan, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj,
            strm.ccal,
            strm.ccic ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPTC354 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.ccal=src.ccal) OR (target.ccal IS NULL AND src.ccal IS NULL)) AND
            ((target.ccic=src.ccic) OR (target.ccic IS NULL AND src.ccic IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.amch_1=src.amch_1, 
                    target.amch_2=src.amch_2, 
                    target.amch_3=src.amch_3, 
                    target.amch_4=src.amch_4, 
                    target.ames_1=src.ames_1, 
                    target.ames_2=src.ames_2, 
                    target.ames_3=src.ames_3, 
                    target.ames_4=src.ames_4, 
                    target.ccal=src.ccal, 
                    target.ccic=src.ccic, 
                    target.compnr=src.compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_ccal_ref_compnr=src.cprj_ccal_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.quan=src.quan, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    amch_1, 
                    amch_2, 
                    amch_3, 
                    amch_4, 
                    ames_1, 
                    ames_2, 
                    ames_3, 
                    ames_4, 
                    ccal, 
                    ccic, 
                    compnr, 
                    cprj, 
                    cprj_ccal_ref_compnr, 
                    cprj_ref_compnr, 
                    deleted, 
                    quan, 
                    sequencenumber, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.amch_1, 
                    src.amch_2, 
                    src.amch_3, 
                    src.amch_4, 
                    src.ames_1, 
                    src.ames_2, 
                    src.ames_3, 
                    src.ames_4, 
                    src.ccal, 
                    src.ccic, 
                    src.compnr, 
                    src.cprj, 
                    src.cprj_ccal_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.deleted, 
                    src.quan, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPTC354_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.amch_1, 
            strm.amch_2, 
            strm.amch_3, 
            strm.amch_4, 
            strm.ames_1, 
            strm.ames_2, 
            strm.ames_3, 
            strm.ames_4, 
            strm.ccal, 
            strm.ccic, 
            strm.compnr, 
            strm.cprj, 
            strm.cprj_ccal_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.deleted, 
            strm.quan, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj,
            strm.ccal,
            strm.ccic ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPTC354 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.ccal=src.ccal) OR (target.ccal IS NULL AND src.ccal IS NULL)) AND
            ((target.ccic=src.ccic) OR (target.ccic IS NULL AND src.ccic IS NULL)) 
                when matched then update set
                    target.amch_1=src.amch_1, 
                    target.amch_2=src.amch_2, 
                    target.amch_3=src.amch_3, 
                    target.amch_4=src.amch_4, 
                    target.ames_1=src.ames_1, 
                    target.ames_2=src.ames_2, 
                    target.ames_3=src.ames_3, 
                    target.ames_4=src.ames_4, 
                    target.ccal=src.ccal, 
                    target.ccic=src.ccic, 
                    target.compnr=src.compnr, 
                    target.cprj=src.cprj, 
                    target.cprj_ccal_ref_compnr=src.cprj_ccal_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.quan=src.quan, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( amch_1, amch_2, amch_3, amch_4, ames_1, ames_2, ames_3, ames_4, ccal, ccic, compnr, cprj, cprj_ccal_ref_compnr, cprj_ref_compnr, deleted, quan, sequencenumber, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.amch_1, 
                    src.amch_2, 
                    src.amch_3, 
                    src.amch_4, 
                    src.ames_1, 
                    src.ames_2, 
                    src.ames_3, 
                    src.ames_4, 
                    src.ccal, 
                    src.ccic, 
                    src.compnr, 
                    src.cprj, 
                    src.cprj_ccal_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.deleted, 
                    src.quan, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username,     
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
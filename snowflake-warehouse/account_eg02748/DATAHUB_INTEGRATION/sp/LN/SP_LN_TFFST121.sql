create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFFST121()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFFST121_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFFST121 as target using (SELECT * FROM (SELECT 
            strm.accn, 
            strm.cmpf, 
            strm.cmpm, 
            strm.cmpt, 
            strm.compnr, 
            strm.deleted, 
            strm.dimf_1, 
            strm.dimf_10, 
            strm.dimf_11, 
            strm.dimf_12, 
            strm.dimf_2, 
            strm.dimf_3, 
            strm.dimf_4, 
            strm.dimf_5, 
            strm.dimf_6, 
            strm.dimf_7, 
            strm.dimf_8, 
            strm.dimf_9, 
            strm.dimm_1, 
            strm.dimm_10, 
            strm.dimm_11, 
            strm.dimm_12, 
            strm.dimm_2, 
            strm.dimm_3, 
            strm.dimm_4, 
            strm.dimm_5, 
            strm.dimm_6, 
            strm.dimm_7, 
            strm.dimm_8, 
            strm.dimm_9, 
            strm.dimt_1, 
            strm.dimt_10, 
            strm.dimt_11, 
            strm.dimt_12, 
            strm.dimt_2, 
            strm.dimt_3, 
            strm.dimt_4, 
            strm.dimt_5, 
            strm.dimt_6, 
            strm.dimt_7, 
            strm.dimt_8, 
            strm.dimt_9, 
            strm.fstm, 
            strm.fstm_accn_ref_compnr, 
            strm.fstm_ref_compnr, 
            strm.lacf, 
            strm.lacm, 
            strm.lact, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.fstm,
            strm.accn,
            strm.seqn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFST121 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.fstm=src.fstm) OR (target.fstm IS NULL AND src.fstm IS NULL)) AND
            ((target.accn=src.accn) OR (target.accn IS NULL AND src.accn IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.accn=src.accn, 
                    target.cmpf=src.cmpf, 
                    target.cmpm=src.cmpm, 
                    target.cmpt=src.cmpt, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dimf_1=src.dimf_1, 
                    target.dimf_10=src.dimf_10, 
                    target.dimf_11=src.dimf_11, 
                    target.dimf_12=src.dimf_12, 
                    target.dimf_2=src.dimf_2, 
                    target.dimf_3=src.dimf_3, 
                    target.dimf_4=src.dimf_4, 
                    target.dimf_5=src.dimf_5, 
                    target.dimf_6=src.dimf_6, 
                    target.dimf_7=src.dimf_7, 
                    target.dimf_8=src.dimf_8, 
                    target.dimf_9=src.dimf_9, 
                    target.dimm_1=src.dimm_1, 
                    target.dimm_10=src.dimm_10, 
                    target.dimm_11=src.dimm_11, 
                    target.dimm_12=src.dimm_12, 
                    target.dimm_2=src.dimm_2, 
                    target.dimm_3=src.dimm_3, 
                    target.dimm_4=src.dimm_4, 
                    target.dimm_5=src.dimm_5, 
                    target.dimm_6=src.dimm_6, 
                    target.dimm_7=src.dimm_7, 
                    target.dimm_8=src.dimm_8, 
                    target.dimm_9=src.dimm_9, 
                    target.dimt_1=src.dimt_1, 
                    target.dimt_10=src.dimt_10, 
                    target.dimt_11=src.dimt_11, 
                    target.dimt_12=src.dimt_12, 
                    target.dimt_2=src.dimt_2, 
                    target.dimt_3=src.dimt_3, 
                    target.dimt_4=src.dimt_4, 
                    target.dimt_5=src.dimt_5, 
                    target.dimt_6=src.dimt_6, 
                    target.dimt_7=src.dimt_7, 
                    target.dimt_8=src.dimt_8, 
                    target.dimt_9=src.dimt_9, 
                    target.fstm=src.fstm, 
                    target.fstm_accn_ref_compnr=src.fstm_accn_ref_compnr, 
                    target.fstm_ref_compnr=src.fstm_ref_compnr, 
                    target.lacf=src.lacf, 
                    target.lacm=src.lacm, 
                    target.lact=src.lact, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    accn, 
                    cmpf, 
                    cmpm, 
                    cmpt, 
                    compnr, 
                    deleted, 
                    dimf_1, 
                    dimf_10, 
                    dimf_11, 
                    dimf_12, 
                    dimf_2, 
                    dimf_3, 
                    dimf_4, 
                    dimf_5, 
                    dimf_6, 
                    dimf_7, 
                    dimf_8, 
                    dimf_9, 
                    dimm_1, 
                    dimm_10, 
                    dimm_11, 
                    dimm_12, 
                    dimm_2, 
                    dimm_3, 
                    dimm_4, 
                    dimm_5, 
                    dimm_6, 
                    dimm_7, 
                    dimm_8, 
                    dimm_9, 
                    dimt_1, 
                    dimt_10, 
                    dimt_11, 
                    dimt_12, 
                    dimt_2, 
                    dimt_3, 
                    dimt_4, 
                    dimt_5, 
                    dimt_6, 
                    dimt_7, 
                    dimt_8, 
                    dimt_9, 
                    fstm, 
                    fstm_accn_ref_compnr, 
                    fstm_ref_compnr, 
                    lacf, 
                    lacm, 
                    lact, 
                    seqn, 
                    sequencenumber, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.accn, 
                    src.cmpf, 
                    src.cmpm, 
                    src.cmpt, 
                    src.compnr, 
                    src.deleted, 
                    src.dimf_1, 
                    src.dimf_10, 
                    src.dimf_11, 
                    src.dimf_12, 
                    src.dimf_2, 
                    src.dimf_3, 
                    src.dimf_4, 
                    src.dimf_5, 
                    src.dimf_6, 
                    src.dimf_7, 
                    src.dimf_8, 
                    src.dimf_9, 
                    src.dimm_1, 
                    src.dimm_10, 
                    src.dimm_11, 
                    src.dimm_12, 
                    src.dimm_2, 
                    src.dimm_3, 
                    src.dimm_4, 
                    src.dimm_5, 
                    src.dimm_6, 
                    src.dimm_7, 
                    src.dimm_8, 
                    src.dimm_9, 
                    src.dimt_1, 
                    src.dimt_10, 
                    src.dimt_11, 
                    src.dimt_12, 
                    src.dimt_2, 
                    src.dimt_3, 
                    src.dimt_4, 
                    src.dimt_5, 
                    src.dimt_6, 
                    src.dimt_7, 
                    src.dimt_8, 
                    src.dimt_9, 
                    src.fstm, 
                    src.fstm_accn_ref_compnr, 
                    src.fstm_ref_compnr, 
                    src.lacf, 
                    src.lacm, 
                    src.lact, 
                    src.seqn, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFFST121_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.accn, 
            strm.cmpf, 
            strm.cmpm, 
            strm.cmpt, 
            strm.compnr, 
            strm.deleted, 
            strm.dimf_1, 
            strm.dimf_10, 
            strm.dimf_11, 
            strm.dimf_12, 
            strm.dimf_2, 
            strm.dimf_3, 
            strm.dimf_4, 
            strm.dimf_5, 
            strm.dimf_6, 
            strm.dimf_7, 
            strm.dimf_8, 
            strm.dimf_9, 
            strm.dimm_1, 
            strm.dimm_10, 
            strm.dimm_11, 
            strm.dimm_12, 
            strm.dimm_2, 
            strm.dimm_3, 
            strm.dimm_4, 
            strm.dimm_5, 
            strm.dimm_6, 
            strm.dimm_7, 
            strm.dimm_8, 
            strm.dimm_9, 
            strm.dimt_1, 
            strm.dimt_10, 
            strm.dimt_11, 
            strm.dimt_12, 
            strm.dimt_2, 
            strm.dimt_3, 
            strm.dimt_4, 
            strm.dimt_5, 
            strm.dimt_6, 
            strm.dimt_7, 
            strm.dimt_8, 
            strm.dimt_9, 
            strm.fstm, 
            strm.fstm_accn_ref_compnr, 
            strm.fstm_ref_compnr, 
            strm.lacf, 
            strm.lacm, 
            strm.lact, 
            strm.seqn, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.fstm,
            strm.accn,
            strm.seqn ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFST121 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.fstm=src.fstm) OR (target.fstm IS NULL AND src.fstm IS NULL)) AND
            ((target.accn=src.accn) OR (target.accn IS NULL AND src.accn IS NULL)) AND
            ((target.seqn=src.seqn) OR (target.seqn IS NULL AND src.seqn IS NULL)) 
                when matched then update set
                    target.accn=src.accn, 
                    target.cmpf=src.cmpf, 
                    target.cmpm=src.cmpm, 
                    target.cmpt=src.cmpt, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.dimf_1=src.dimf_1, 
                    target.dimf_10=src.dimf_10, 
                    target.dimf_11=src.dimf_11, 
                    target.dimf_12=src.dimf_12, 
                    target.dimf_2=src.dimf_2, 
                    target.dimf_3=src.dimf_3, 
                    target.dimf_4=src.dimf_4, 
                    target.dimf_5=src.dimf_5, 
                    target.dimf_6=src.dimf_6, 
                    target.dimf_7=src.dimf_7, 
                    target.dimf_8=src.dimf_8, 
                    target.dimf_9=src.dimf_9, 
                    target.dimm_1=src.dimm_1, 
                    target.dimm_10=src.dimm_10, 
                    target.dimm_11=src.dimm_11, 
                    target.dimm_12=src.dimm_12, 
                    target.dimm_2=src.dimm_2, 
                    target.dimm_3=src.dimm_3, 
                    target.dimm_4=src.dimm_4, 
                    target.dimm_5=src.dimm_5, 
                    target.dimm_6=src.dimm_6, 
                    target.dimm_7=src.dimm_7, 
                    target.dimm_8=src.dimm_8, 
                    target.dimm_9=src.dimm_9, 
                    target.dimt_1=src.dimt_1, 
                    target.dimt_10=src.dimt_10, 
                    target.dimt_11=src.dimt_11, 
                    target.dimt_12=src.dimt_12, 
                    target.dimt_2=src.dimt_2, 
                    target.dimt_3=src.dimt_3, 
                    target.dimt_4=src.dimt_4, 
                    target.dimt_5=src.dimt_5, 
                    target.dimt_6=src.dimt_6, 
                    target.dimt_7=src.dimt_7, 
                    target.dimt_8=src.dimt_8, 
                    target.dimt_9=src.dimt_9, 
                    target.fstm=src.fstm, 
                    target.fstm_accn_ref_compnr=src.fstm_accn_ref_compnr, 
                    target.fstm_ref_compnr=src.fstm_ref_compnr, 
                    target.lacf=src.lacf, 
                    target.lacm=src.lacm, 
                    target.lact=src.lact, 
                    target.seqn=src.seqn, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( accn, cmpf, cmpm, cmpt, compnr, deleted, dimf_1, dimf_10, dimf_11, dimf_12, dimf_2, dimf_3, dimf_4, dimf_5, dimf_6, dimf_7, dimf_8, dimf_9, dimm_1, dimm_10, dimm_11, dimm_12, dimm_2, dimm_3, dimm_4, dimm_5, dimm_6, dimm_7, dimm_8, dimm_9, dimt_1, dimt_10, dimt_11, dimt_12, dimt_2, dimt_3, dimt_4, dimt_5, dimt_6, dimt_7, dimt_8, dimt_9, fstm, fstm_accn_ref_compnr, fstm_ref_compnr, lacf, lacm, lact, seqn, sequencenumber, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.accn, 
                    src.cmpf, 
                    src.cmpm, 
                    src.cmpt, 
                    src.compnr, 
                    src.deleted, 
                    src.dimf_1, 
                    src.dimf_10, 
                    src.dimf_11, 
                    src.dimf_12, 
                    src.dimf_2, 
                    src.dimf_3, 
                    src.dimf_4, 
                    src.dimf_5, 
                    src.dimf_6, 
                    src.dimf_7, 
                    src.dimf_8, 
                    src.dimf_9, 
                    src.dimm_1, 
                    src.dimm_10, 
                    src.dimm_11, 
                    src.dimm_12, 
                    src.dimm_2, 
                    src.dimm_3, 
                    src.dimm_4, 
                    src.dimm_5, 
                    src.dimm_6, 
                    src.dimm_7, 
                    src.dimm_8, 
                    src.dimm_9, 
                    src.dimt_1, 
                    src.dimt_10, 
                    src.dimt_11, 
                    src.dimt_12, 
                    src.dimt_2, 
                    src.dimt_3, 
                    src.dimt_4, 
                    src.dimt_5, 
                    src.dimt_6, 
                    src.dimt_7, 
                    src.dimt_8, 
                    src.dimt_9, 
                    src.fstm, 
                    src.fstm_accn_ref_compnr, 
                    src.fstm_ref_compnr, 
                    src.lacf, 
                    src.lacm, 
                    src.lact, 
                    src.seqn, 
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
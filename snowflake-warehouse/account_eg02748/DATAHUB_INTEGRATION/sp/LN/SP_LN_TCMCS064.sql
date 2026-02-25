create or replace procedure DATAHUB_INTEGRATION.SP_LN_TCMCS064()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TCMCS064_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TCMCS064 as target using (SELECT * FROM (SELECT 
            strm.actn, 
            strm.actn_kw, 
            strm.compnr, 
            strm.crat, 
            strm.ddcc, 
            strm.ddcc_kw, 
            strm.deleted, 
            strm.dsca, 
            strm.ioso, 
            strm.ioso_kw, 
            strm.phs1, 
            strm.phs1_kw, 
            strm.phs2, 
            strm.phs2_kw, 
            strm.phs3, 
            strm.phs3_kw, 
            strm.phs4, 
            strm.phs4_kw, 
            strm.phs5, 
            strm.phs5_kw, 
            strm.phs6, 
            strm.phs6_kw, 
            strm.phs7, 
            strm.phs7_kw, 
            strm.revp, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.crat ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCMCS064 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.crat=src.crat) OR (target.crat IS NULL AND src.crat IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.actn=src.actn, 
                    target.actn_kw=src.actn_kw, 
                    target.compnr=src.compnr, 
                    target.crat=src.crat, 
                    target.ddcc=src.ddcc, 
                    target.ddcc_kw=src.ddcc_kw, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.ioso=src.ioso, 
                    target.ioso_kw=src.ioso_kw, 
                    target.phs1=src.phs1, 
                    target.phs1_kw=src.phs1_kw, 
                    target.phs2=src.phs2, 
                    target.phs2_kw=src.phs2_kw, 
                    target.phs3=src.phs3, 
                    target.phs3_kw=src.phs3_kw, 
                    target.phs4=src.phs4, 
                    target.phs4_kw=src.phs4_kw, 
                    target.phs5=src.phs5, 
                    target.phs5_kw=src.phs5_kw, 
                    target.phs6=src.phs6, 
                    target.phs6_kw=src.phs6_kw, 
                    target.phs7=src.phs7, 
                    target.phs7_kw=src.phs7_kw, 
                    target.revp=src.revp, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    actn, 
                    actn_kw, 
                    compnr, 
                    crat, 
                    ddcc, 
                    ddcc_kw, 
                    deleted, 
                    dsca, 
                    ioso, 
                    ioso_kw, 
                    phs1, 
                    phs1_kw, 
                    phs2, 
                    phs2_kw, 
                    phs3, 
                    phs3_kw, 
                    phs4, 
                    phs4_kw, 
                    phs5, 
                    phs5_kw, 
                    phs6, 
                    phs6_kw, 
                    phs7, 
                    phs7_kw, 
                    revp, 
                    sequencenumber, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.actn, 
                    src.actn_kw, 
                    src.compnr, 
                    src.crat, 
                    src.ddcc, 
                    src.ddcc_kw, 
                    src.deleted, 
                    src.dsca, 
                    src.ioso, 
                    src.ioso_kw, 
                    src.phs1, 
                    src.phs1_kw, 
                    src.phs2, 
                    src.phs2_kw, 
                    src.phs3, 
                    src.phs3_kw, 
                    src.phs4, 
                    src.phs4_kw, 
                    src.phs5, 
                    src.phs5_kw, 
                    src.phs6, 
                    src.phs6_kw, 
                    src.phs7, 
                    src.phs7_kw, 
                    src.revp, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TCMCS064_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.actn, 
            strm.actn_kw, 
            strm.compnr, 
            strm.crat, 
            strm.ddcc, 
            strm.ddcc_kw, 
            strm.deleted, 
            strm.dsca, 
            strm.ioso, 
            strm.ioso_kw, 
            strm.phs1, 
            strm.phs1_kw, 
            strm.phs2, 
            strm.phs2_kw, 
            strm.phs3, 
            strm.phs3_kw, 
            strm.phs4, 
            strm.phs4_kw, 
            strm.phs5, 
            strm.phs5_kw, 
            strm.phs6, 
            strm.phs6_kw, 
            strm.phs7, 
            strm.phs7_kw, 
            strm.revp, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.crat ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TCMCS064 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.crat=src.crat) OR (target.crat IS NULL AND src.crat IS NULL)) 
                when matched then update set
                    target.actn=src.actn, 
                    target.actn_kw=src.actn_kw, 
                    target.compnr=src.compnr, 
                    target.crat=src.crat, 
                    target.ddcc=src.ddcc, 
                    target.ddcc_kw=src.ddcc_kw, 
                    target.deleted=src.deleted, 
                    target.dsca=src.dsca, 
                    target.ioso=src.ioso, 
                    target.ioso_kw=src.ioso_kw, 
                    target.phs1=src.phs1, 
                    target.phs1_kw=src.phs1_kw, 
                    target.phs2=src.phs2, 
                    target.phs2_kw=src.phs2_kw, 
                    target.phs3=src.phs3, 
                    target.phs3_kw=src.phs3_kw, 
                    target.phs4=src.phs4, 
                    target.phs4_kw=src.phs4_kw, 
                    target.phs5=src.phs5, 
                    target.phs5_kw=src.phs5_kw, 
                    target.phs6=src.phs6, 
                    target.phs6_kw=src.phs6_kw, 
                    target.phs7=src.phs7, 
                    target.phs7_kw=src.phs7_kw, 
                    target.revp=src.revp, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( actn, actn_kw, compnr, crat, ddcc, ddcc_kw, deleted, dsca, ioso, ioso_kw, phs1, phs1_kw, phs2, phs2_kw, phs3, phs3_kw, phs4, phs4_kw, phs5, phs5_kw, phs6, phs6_kw, phs7, phs7_kw, revp, sequencenumber, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.actn, 
                    src.actn_kw, 
                    src.compnr, 
                    src.crat, 
                    src.ddcc, 
                    src.ddcc_kw, 
                    src.deleted, 
                    src.dsca, 
                    src.ioso, 
                    src.ioso_kw, 
                    src.phs1, 
                    src.phs1_kw, 
                    src.phs2, 
                    src.phs2_kw, 
                    src.phs3, 
                    src.phs3_kw, 
                    src.phs4, 
                    src.phs4_kw, 
                    src.phs5, 
                    src.phs5_kw, 
                    src.phs6, 
                    src.phs6_kw, 
                    src.phs7, 
                    src.phs7_kw, 
                    src.revp, 
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
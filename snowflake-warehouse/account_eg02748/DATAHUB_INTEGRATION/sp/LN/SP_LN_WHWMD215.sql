create or replace procedure DATAHUB_INTEGRATION.SP_LN_WHWMD215()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_WHWMD215_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_WHWMD215 as target using (SELECT * FROM (SELECT 
            strm.compnr, 
            strm.cwar, 
            strm.cwar_item_ref_compnr, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.hstd, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.lsid, 
            strm.ltdt, 
            strm.qall, 
            strm.qblk, 
            strm.qbpl, 
            strm.qcal, 
            strm.qchd, 
            strm.qcis, 
            strm.qcit, 
            strm.qcom, 
            strm.qcor, 
            strm.qcpr, 
            strm.qcrj, 
            strm.qgit, 
            strm.qhib, 
            strm.qhnd, 
            strm.qhrj, 
            strm.qint, 
            strm.qlal, 
            strm.qnal, 
            strm.qnbl, 
            strm.qnbp, 
            strm.qnhd, 
            strm.qnit, 
            strm.qnor, 
            strm.qnrj, 
            strm.qoal, 
            strm.qoor, 
            strm.qord, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cwar,
            strm.item ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHWMD215 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cwar=src.cwar) OR (target.cwar IS NULL AND src.cwar IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.compnr=src.compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_item_ref_compnr=src.cwar_item_ref_compnr, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.hstd=src.hstd, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.lsid=src.lsid, 
                    target.ltdt=src.ltdt, 
                    target.qall=src.qall, 
                    target.qblk=src.qblk, 
                    target.qbpl=src.qbpl, 
                    target.qcal=src.qcal, 
                    target.qchd=src.qchd, 
                    target.qcis=src.qcis, 
                    target.qcit=src.qcit, 
                    target.qcom=src.qcom, 
                    target.qcor=src.qcor, 
                    target.qcpr=src.qcpr, 
                    target.qcrj=src.qcrj, 
                    target.qgit=src.qgit, 
                    target.qhib=src.qhib, 
                    target.qhnd=src.qhnd, 
                    target.qhrj=src.qhrj, 
                    target.qint=src.qint, 
                    target.qlal=src.qlal, 
                    target.qnal=src.qnal, 
                    target.qnbl=src.qnbl, 
                    target.qnbp=src.qnbp, 
                    target.qnhd=src.qnhd, 
                    target.qnit=src.qnit, 
                    target.qnor=src.qnor, 
                    target.qnrj=src.qnrj, 
                    target.qoal=src.qoal, 
                    target.qoor=src.qoor, 
                    target.qord=src.qord, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    compnr, 
                    cwar, 
                    cwar_item_ref_compnr, 
                    cwar_ref_compnr, 
                    deleted, 
                    hstd, 
                    item, 
                    item_ref_compnr, 
                    lsid, 
                    ltdt, 
                    qall, 
                    qblk, 
                    qbpl, 
                    qcal, 
                    qchd, 
                    qcis, 
                    qcit, 
                    qcom, 
                    qcor, 
                    qcpr, 
                    qcrj, 
                    qgit, 
                    qhib, 
                    qhnd, 
                    qhrj, 
                    qint, 
                    qlal, 
                    qnal, 
                    qnbl, 
                    qnbp, 
                    qnhd, 
                    qnit, 
                    qnor, 
                    qnrj, 
                    qoal, 
                    qoor, 
                    qord, 
                    sequencenumber, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.compnr, 
                    src.cwar, 
                    src.cwar_item_ref_compnr, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.hstd, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.lsid, 
                    src.ltdt, 
                    src.qall, 
                    src.qblk, 
                    src.qbpl, 
                    src.qcal, 
                    src.qchd, 
                    src.qcis, 
                    src.qcit, 
                    src.qcom, 
                    src.qcor, 
                    src.qcpr, 
                    src.qcrj, 
                    src.qgit, 
                    src.qhib, 
                    src.qhnd, 
                    src.qhrj, 
                    src.qint, 
                    src.qlal, 
                    src.qnal, 
                    src.qnbl, 
                    src.qnbp, 
                    src.qnhd, 
                    src.qnit, 
                    src.qnor, 
                    src.qnrj, 
                    src.qoal, 
                    src.qoor, 
                    src.qord, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_WHWMD215_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.compnr, 
            strm.cwar, 
            strm.cwar_item_ref_compnr, 
            strm.cwar_ref_compnr, 
            strm.deleted, 
            strm.hstd, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.lsid, 
            strm.ltdt, 
            strm.qall, 
            strm.qblk, 
            strm.qbpl, 
            strm.qcal, 
            strm.qchd, 
            strm.qcis, 
            strm.qcit, 
            strm.qcom, 
            strm.qcor, 
            strm.qcpr, 
            strm.qcrj, 
            strm.qgit, 
            strm.qhib, 
            strm.qhnd, 
            strm.qhrj, 
            strm.qint, 
            strm.qlal, 
            strm.qnal, 
            strm.qnbl, 
            strm.qnbp, 
            strm.qnhd, 
            strm.qnit, 
            strm.qnor, 
            strm.qnrj, 
            strm.qoal, 
            strm.qoor, 
            strm.qord, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cwar,
            strm.item ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_WHWMD215 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cwar=src.cwar) OR (target.cwar IS NULL AND src.cwar IS NULL)) AND
            ((target.item=src.item) OR (target.item IS NULL AND src.item IS NULL)) 
                when matched then update set
                    target.compnr=src.compnr, 
                    target.cwar=src.cwar, 
                    target.cwar_item_ref_compnr=src.cwar_item_ref_compnr, 
                    target.cwar_ref_compnr=src.cwar_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.hstd=src.hstd, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.lsid=src.lsid, 
                    target.ltdt=src.ltdt, 
                    target.qall=src.qall, 
                    target.qblk=src.qblk, 
                    target.qbpl=src.qbpl, 
                    target.qcal=src.qcal, 
                    target.qchd=src.qchd, 
                    target.qcis=src.qcis, 
                    target.qcit=src.qcit, 
                    target.qcom=src.qcom, 
                    target.qcor=src.qcor, 
                    target.qcpr=src.qcpr, 
                    target.qcrj=src.qcrj, 
                    target.qgit=src.qgit, 
                    target.qhib=src.qhib, 
                    target.qhnd=src.qhnd, 
                    target.qhrj=src.qhrj, 
                    target.qint=src.qint, 
                    target.qlal=src.qlal, 
                    target.qnal=src.qnal, 
                    target.qnbl=src.qnbl, 
                    target.qnbp=src.qnbp, 
                    target.qnhd=src.qnhd, 
                    target.qnit=src.qnit, 
                    target.qnor=src.qnor, 
                    target.qnrj=src.qnrj, 
                    target.qoal=src.qoal, 
                    target.qoor=src.qoor, 
                    target.qord=src.qord, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( compnr, cwar, cwar_item_ref_compnr, cwar_ref_compnr, deleted, hstd, item, item_ref_compnr, lsid, ltdt, qall, qblk, qbpl, qcal, qchd, qcis, qcit, qcom, qcor, qcpr, qcrj, qgit, qhib, qhnd, qhrj, qint, qlal, qnal, qnbl, qnbp, qnhd, qnit, qnor, qnrj, qoal, qoor, qord, sequencenumber, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.compnr, 
                    src.cwar, 
                    src.cwar_item_ref_compnr, 
                    src.cwar_ref_compnr, 
                    src.deleted, 
                    src.hstd, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.lsid, 
                    src.ltdt, 
                    src.qall, 
                    src.qblk, 
                    src.qbpl, 
                    src.qcal, 
                    src.qchd, 
                    src.qcis, 
                    src.qcit, 
                    src.qcom, 
                    src.qcor, 
                    src.qcpr, 
                    src.qcrj, 
                    src.qgit, 
                    src.qhib, 
                    src.qhnd, 
                    src.qhrj, 
                    src.qint, 
                    src.qlal, 
                    src.qnal, 
                    src.qnbl, 
                    src.qnbp, 
                    src.qnhd, 
                    src.qnit, 
                    src.qnor, 
                    src.qnrj, 
                    src.qoal, 
                    src.qoor, 
                    src.qord, 
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
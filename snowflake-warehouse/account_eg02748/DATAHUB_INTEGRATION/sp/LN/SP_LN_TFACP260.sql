create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFACP260()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFACP260_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFACP260 as target using (SELECT * FROM (SELECT 
            strm.byer, 
            strm.clus, 
            strm.compnr, 
            strm.deleted, 
            strm.idoc, 
            strm.ityp, 
            strm.load, 
            strm.loco, 
            strm.logd, 
            strm.logt, 
            strm.mess, 
            strm.orno, 
            strm.otyp, 
            strm.otyp_kw, 
            strm.pono, 
            strm.sequencenumber, 
            strm.shpi, 
            strm.sqnb, 
            strm.timestamp, 
            strm.user, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.ityp,
            strm.idoc,
            strm.loco,
            strm.otyp,
            strm.orno,
            strm.pono,
            strm.sqnb ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP260 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.ityp=src.ityp) OR (target.ityp IS NULL AND src.ityp IS NULL)) AND
            ((target.idoc=src.idoc) OR (target.idoc IS NULL AND src.idoc IS NULL)) AND
            ((target.loco=src.loco) OR (target.loco IS NULL AND src.loco IS NULL)) AND
            ((target.otyp=src.otyp) OR (target.otyp IS NULL AND src.otyp IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.pono=src.pono) OR (target.pono IS NULL AND src.pono IS NULL)) AND
            ((target.sqnb=src.sqnb) OR (target.sqnb IS NULL AND src.sqnb IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.byer=src.byer, 
                    target.clus=src.clus, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.idoc=src.idoc, 
                    target.ityp=src.ityp, 
                    target.load=src.load, 
                    target.loco=src.loco, 
                    target.logd=src.logd, 
                    target.logt=src.logt, 
                    target.mess=src.mess, 
                    target.orno=src.orno, 
                    target.otyp=src.otyp, 
                    target.otyp_kw=src.otyp_kw, 
                    target.pono=src.pono, 
                    target.sequencenumber=src.sequencenumber, 
                    target.shpi=src.shpi, 
                    target.sqnb=src.sqnb, 
                    target.timestamp=src.timestamp, 
                    target.user=src.user, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    byer, 
                    clus, 
                    compnr, 
                    deleted, 
                    idoc, 
                    ityp, 
                    load, 
                    loco, 
                    logd, 
                    logt, 
                    mess, 
                    orno, 
                    otyp, 
                    otyp_kw, 
                    pono, 
                    sequencenumber, 
                    shpi, 
                    sqnb, 
                    timestamp, 
                    user, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.byer, 
                    src.clus, 
                    src.compnr, 
                    src.deleted, 
                    src.idoc, 
                    src.ityp, 
                    src.load, 
                    src.loco, 
                    src.logd, 
                    src.logt, 
                    src.mess, 
                    src.orno, 
                    src.otyp, 
                    src.otyp_kw, 
                    src.pono, 
                    src.sequencenumber, 
                    src.shpi, 
                    src.sqnb, 
                    src.timestamp, 
                    src.user, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFACP260_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.byer, 
            strm.clus, 
            strm.compnr, 
            strm.deleted, 
            strm.idoc, 
            strm.ityp, 
            strm.load, 
            strm.loco, 
            strm.logd, 
            strm.logt, 
            strm.mess, 
            strm.orno, 
            strm.otyp, 
            strm.otyp_kw, 
            strm.pono, 
            strm.sequencenumber, 
            strm.shpi, 
            strm.sqnb, 
            strm.timestamp, 
            strm.user, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.ityp,
            strm.idoc,
            strm.loco,
            strm.otyp,
            strm.orno,
            strm.pono,
            strm.sqnb ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP260 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.ityp=src.ityp) OR (target.ityp IS NULL AND src.ityp IS NULL)) AND
            ((target.idoc=src.idoc) OR (target.idoc IS NULL AND src.idoc IS NULL)) AND
            ((target.loco=src.loco) OR (target.loco IS NULL AND src.loco IS NULL)) AND
            ((target.otyp=src.otyp) OR (target.otyp IS NULL AND src.otyp IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.pono=src.pono) OR (target.pono IS NULL AND src.pono IS NULL)) AND
            ((target.sqnb=src.sqnb) OR (target.sqnb IS NULL AND src.sqnb IS NULL)) 
                when matched then update set
                    target.byer=src.byer, 
                    target.clus=src.clus, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.idoc=src.idoc, 
                    target.ityp=src.ityp, 
                    target.load=src.load, 
                    target.loco=src.loco, 
                    target.logd=src.logd, 
                    target.logt=src.logt, 
                    target.mess=src.mess, 
                    target.orno=src.orno, 
                    target.otyp=src.otyp, 
                    target.otyp_kw=src.otyp_kw, 
                    target.pono=src.pono, 
                    target.sequencenumber=src.sequencenumber, 
                    target.shpi=src.shpi, 
                    target.sqnb=src.sqnb, 
                    target.timestamp=src.timestamp, 
                    target.user=src.user, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( byer, clus, compnr, deleted, idoc, ityp, load, loco, logd, logt, mess, orno, otyp, otyp_kw, pono, sequencenumber, shpi, sqnb, timestamp, user, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.byer, 
                    src.clus, 
                    src.compnr, 
                    src.deleted, 
                    src.idoc, 
                    src.ityp, 
                    src.load, 
                    src.loco, 
                    src.logd, 
                    src.logt, 
                    src.mess, 
                    src.orno, 
                    src.otyp, 
                    src.otyp_kw, 
                    src.pono, 
                    src.sequencenumber, 
                    src.shpi, 
                    src.sqnb, 
                    src.timestamp, 
                    src.user, 
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
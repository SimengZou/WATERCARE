create or replace procedure DATAHUB_INTEGRATION.SP_LN_TSCLM330()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TSCLM330_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TSCLM330 as target using (SELECT * FROM (SELECT 
            strm.compnr, 
            strm.cprl, 
            strm.deleted, 
            strm.desc, 
            strm.epyn, 
            strm.epyn_kw, 
            strm.esyn, 
            strm.esyn_kw, 
            strm.prpr, 
            strm.prpr_ref_compnr, 
            strm.rpyn, 
            strm.rpyn_kw, 
            strm.sear, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.umtc, 
            strm.umtc_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprl ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCLM330 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprl=src.cprl) OR (target.cprl IS NULL AND src.cprl IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.compnr=src.compnr, 
                    target.cprl=src.cprl, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.epyn=src.epyn, 
                    target.epyn_kw=src.epyn_kw, 
                    target.esyn=src.esyn, 
                    target.esyn_kw=src.esyn_kw, 
                    target.prpr=src.prpr, 
                    target.prpr_ref_compnr=src.prpr_ref_compnr, 
                    target.rpyn=src.rpyn, 
                    target.rpyn_kw=src.rpyn_kw, 
                    target.sear=src.sear, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.umtc=src.umtc, 
                    target.umtc_kw=src.umtc_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    compnr, 
                    cprl, 
                    deleted, 
                    desc, 
                    epyn, 
                    epyn_kw, 
                    esyn, 
                    esyn_kw, 
                    prpr, 
                    prpr_ref_compnr, 
                    rpyn, 
                    rpyn_kw, 
                    sear, 
                    sequencenumber, 
                    timestamp, 
                    umtc, 
                    umtc_kw, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.compnr, 
                    src.cprl, 
                    src.deleted, 
                    src.desc, 
                    src.epyn, 
                    src.epyn_kw, 
                    src.esyn, 
                    src.esyn_kw, 
                    src.prpr, 
                    src.prpr_ref_compnr, 
                    src.rpyn, 
                    src.rpyn_kw, 
                    src.sear, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.umtc, 
                    src.umtc_kw, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TSCLM330_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.compnr, 
            strm.cprl, 
            strm.deleted, 
            strm.desc, 
            strm.epyn, 
            strm.epyn_kw, 
            strm.esyn, 
            strm.esyn_kw, 
            strm.prpr, 
            strm.prpr_ref_compnr, 
            strm.rpyn, 
            strm.rpyn_kw, 
            strm.sear, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.umtc, 
            strm.umtc_kw, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprl ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TSCLM330 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprl=src.cprl) OR (target.cprl IS NULL AND src.cprl IS NULL)) 
                when matched then update set
                    target.compnr=src.compnr, 
                    target.cprl=src.cprl, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.epyn=src.epyn, 
                    target.epyn_kw=src.epyn_kw, 
                    target.esyn=src.esyn, 
                    target.esyn_kw=src.esyn_kw, 
                    target.prpr=src.prpr, 
                    target.prpr_ref_compnr=src.prpr_ref_compnr, 
                    target.rpyn=src.rpyn, 
                    target.rpyn_kw=src.rpyn_kw, 
                    target.sear=src.sear, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.umtc=src.umtc, 
                    target.umtc_kw=src.umtc_kw, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( compnr, cprl, deleted, desc, epyn, epyn_kw, esyn, esyn_kw, prpr, prpr_ref_compnr, rpyn, rpyn_kw, sear, sequencenumber, timestamp, umtc, umtc_kw, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.compnr, 
                    src.cprl, 
                    src.deleted, 
                    src.desc, 
                    src.epyn, 
                    src.epyn_kw, 
                    src.esyn, 
                    src.esyn_kw, 
                    src.prpr, 
                    src.prpr_ref_compnr, 
                    src.rpyn, 
                    src.rpyn_kw, 
                    src.sear, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.umtc, 
                    src.umtc_kw, 
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
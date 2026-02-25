create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFGLD172()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFGLD172_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFGLD172 as target using (SELECT * FROM (SELECT 
            strm.acid, 
            strm.compnr, 
            strm.deleted, 
            strm.fcom, 
            strm.fdim_1, 
            strm.fdim_10, 
            strm.fdim_11, 
            strm.fdim_12, 
            strm.fdim_2, 
            strm.fdim_3, 
            strm.fdim_4, 
            strm.fdim_5, 
            strm.fdim_6, 
            strm.fdim_7, 
            strm.fdim_8, 
            strm.fdim_9, 
            strm.flac, 
            strm.line, 
            strm.sequencenumber, 
            strm.taxo, 
            strm.taxo_vers_acid_ref_compnr, 
            strm.tcom, 
            strm.tdim_1, 
            strm.tdim_10, 
            strm.tdim_11, 
            strm.tdim_12, 
            strm.tdim_2, 
            strm.tdim_3, 
            strm.tdim_4, 
            strm.tdim_5, 
            strm.tdim_6, 
            strm.tdim_7, 
            strm.tdim_8, 
            strm.tdim_9, 
            strm.timestamp, 
            strm.tlac, 
            strm.username, 
            strm.vers, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.taxo,
            strm.vers,
            strm.acid,
            strm.line ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD172 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.taxo=src.taxo) OR (target.taxo IS NULL AND src.taxo IS NULL)) AND
            ((target.vers=src.vers) OR (target.vers IS NULL AND src.vers IS NULL)) AND
            ((target.acid=src.acid) OR (target.acid IS NULL AND src.acid IS NULL)) AND
            ((target.line=src.line) OR (target.line IS NULL AND src.line IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.acid=src.acid, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.fcom=src.fcom, 
                    target.fdim_1=src.fdim_1, 
                    target.fdim_10=src.fdim_10, 
                    target.fdim_11=src.fdim_11, 
                    target.fdim_12=src.fdim_12, 
                    target.fdim_2=src.fdim_2, 
                    target.fdim_3=src.fdim_3, 
                    target.fdim_4=src.fdim_4, 
                    target.fdim_5=src.fdim_5, 
                    target.fdim_6=src.fdim_6, 
                    target.fdim_7=src.fdim_7, 
                    target.fdim_8=src.fdim_8, 
                    target.fdim_9=src.fdim_9, 
                    target.flac=src.flac, 
                    target.line=src.line, 
                    target.sequencenumber=src.sequencenumber, 
                    target.taxo=src.taxo, 
                    target.taxo_vers_acid_ref_compnr=src.taxo_vers_acid_ref_compnr, 
                    target.tcom=src.tcom, 
                    target.tdim_1=src.tdim_1, 
                    target.tdim_10=src.tdim_10, 
                    target.tdim_11=src.tdim_11, 
                    target.tdim_12=src.tdim_12, 
                    target.tdim_2=src.tdim_2, 
                    target.tdim_3=src.tdim_3, 
                    target.tdim_4=src.tdim_4, 
                    target.tdim_5=src.tdim_5, 
                    target.tdim_6=src.tdim_6, 
                    target.tdim_7=src.tdim_7, 
                    target.tdim_8=src.tdim_8, 
                    target.tdim_9=src.tdim_9, 
                    target.timestamp=src.timestamp, 
                    target.tlac=src.tlac, 
                    target.username=src.username, 
                    target.vers=src.vers, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    acid, 
                    compnr, 
                    deleted, 
                    fcom, 
                    fdim_1, 
                    fdim_10, 
                    fdim_11, 
                    fdim_12, 
                    fdim_2, 
                    fdim_3, 
                    fdim_4, 
                    fdim_5, 
                    fdim_6, 
                    fdim_7, 
                    fdim_8, 
                    fdim_9, 
                    flac, 
                    line, 
                    sequencenumber, 
                    taxo, 
                    taxo_vers_acid_ref_compnr, 
                    tcom, 
                    tdim_1, 
                    tdim_10, 
                    tdim_11, 
                    tdim_12, 
                    tdim_2, 
                    tdim_3, 
                    tdim_4, 
                    tdim_5, 
                    tdim_6, 
                    tdim_7, 
                    tdim_8, 
                    tdim_9, 
                    timestamp, 
                    tlac, 
                    username, 
                    vers, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.acid, 
                    src.compnr, 
                    src.deleted, 
                    src.fcom, 
                    src.fdim_1, 
                    src.fdim_10, 
                    src.fdim_11, 
                    src.fdim_12, 
                    src.fdim_2, 
                    src.fdim_3, 
                    src.fdim_4, 
                    src.fdim_5, 
                    src.fdim_6, 
                    src.fdim_7, 
                    src.fdim_8, 
                    src.fdim_9, 
                    src.flac, 
                    src.line, 
                    src.sequencenumber, 
                    src.taxo, 
                    src.taxo_vers_acid_ref_compnr, 
                    src.tcom, 
                    src.tdim_1, 
                    src.tdim_10, 
                    src.tdim_11, 
                    src.tdim_12, 
                    src.tdim_2, 
                    src.tdim_3, 
                    src.tdim_4, 
                    src.tdim_5, 
                    src.tdim_6, 
                    src.tdim_7, 
                    src.tdim_8, 
                    src.tdim_9, 
                    src.timestamp, 
                    src.tlac, 
                    src.username, 
                    src.vers,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFGLD172_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.acid, 
            strm.compnr, 
            strm.deleted, 
            strm.fcom, 
            strm.fdim_1, 
            strm.fdim_10, 
            strm.fdim_11, 
            strm.fdim_12, 
            strm.fdim_2, 
            strm.fdim_3, 
            strm.fdim_4, 
            strm.fdim_5, 
            strm.fdim_6, 
            strm.fdim_7, 
            strm.fdim_8, 
            strm.fdim_9, 
            strm.flac, 
            strm.line, 
            strm.sequencenumber, 
            strm.taxo, 
            strm.taxo_vers_acid_ref_compnr, 
            strm.tcom, 
            strm.tdim_1, 
            strm.tdim_10, 
            strm.tdim_11, 
            strm.tdim_12, 
            strm.tdim_2, 
            strm.tdim_3, 
            strm.tdim_4, 
            strm.tdim_5, 
            strm.tdim_6, 
            strm.tdim_7, 
            strm.tdim_8, 
            strm.tdim_9, 
            strm.timestamp, 
            strm.tlac, 
            strm.username, 
            strm.vers, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.taxo,
            strm.vers,
            strm.acid,
            strm.line ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD172 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.taxo=src.taxo) OR (target.taxo IS NULL AND src.taxo IS NULL)) AND
            ((target.vers=src.vers) OR (target.vers IS NULL AND src.vers IS NULL)) AND
            ((target.acid=src.acid) OR (target.acid IS NULL AND src.acid IS NULL)) AND
            ((target.line=src.line) OR (target.line IS NULL AND src.line IS NULL)) 
                when matched then update set
                    target.acid=src.acid, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.fcom=src.fcom, 
                    target.fdim_1=src.fdim_1, 
                    target.fdim_10=src.fdim_10, 
                    target.fdim_11=src.fdim_11, 
                    target.fdim_12=src.fdim_12, 
                    target.fdim_2=src.fdim_2, 
                    target.fdim_3=src.fdim_3, 
                    target.fdim_4=src.fdim_4, 
                    target.fdim_5=src.fdim_5, 
                    target.fdim_6=src.fdim_6, 
                    target.fdim_7=src.fdim_7, 
                    target.fdim_8=src.fdim_8, 
                    target.fdim_9=src.fdim_9, 
                    target.flac=src.flac, 
                    target.line=src.line, 
                    target.sequencenumber=src.sequencenumber, 
                    target.taxo=src.taxo, 
                    target.taxo_vers_acid_ref_compnr=src.taxo_vers_acid_ref_compnr, 
                    target.tcom=src.tcom, 
                    target.tdim_1=src.tdim_1, 
                    target.tdim_10=src.tdim_10, 
                    target.tdim_11=src.tdim_11, 
                    target.tdim_12=src.tdim_12, 
                    target.tdim_2=src.tdim_2, 
                    target.tdim_3=src.tdim_3, 
                    target.tdim_4=src.tdim_4, 
                    target.tdim_5=src.tdim_5, 
                    target.tdim_6=src.tdim_6, 
                    target.tdim_7=src.tdim_7, 
                    target.tdim_8=src.tdim_8, 
                    target.tdim_9=src.tdim_9, 
                    target.timestamp=src.timestamp, 
                    target.tlac=src.tlac, 
                    target.username=src.username, 
                    target.vers=src.vers, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( acid, compnr, deleted, fcom, fdim_1, fdim_10, fdim_11, fdim_12, fdim_2, fdim_3, fdim_4, fdim_5, fdim_6, fdim_7, fdim_8, fdim_9, flac, line, sequencenumber, taxo, taxo_vers_acid_ref_compnr, tcom, tdim_1, tdim_10, tdim_11, tdim_12, tdim_2, tdim_3, tdim_4, tdim_5, tdim_6, tdim_7, tdim_8, tdim_9, timestamp, tlac, username, vers, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.acid, 
                    src.compnr, 
                    src.deleted, 
                    src.fcom, 
                    src.fdim_1, 
                    src.fdim_10, 
                    src.fdim_11, 
                    src.fdim_12, 
                    src.fdim_2, 
                    src.fdim_3, 
                    src.fdim_4, 
                    src.fdim_5, 
                    src.fdim_6, 
                    src.fdim_7, 
                    src.fdim_8, 
                    src.fdim_9, 
                    src.flac, 
                    src.line, 
                    src.sequencenumber, 
                    src.taxo, 
                    src.taxo_vers_acid_ref_compnr, 
                    src.tcom, 
                    src.tdim_1, 
                    src.tdim_10, 
                    src.tdim_11, 
                    src.tdim_12, 
                    src.tdim_2, 
                    src.tdim_3, 
                    src.tdim_4, 
                    src.tdim_5, 
                    src.tdim_6, 
                    src.tdim_7, 
                    src.tdim_8, 
                    src.tdim_9, 
                    src.timestamp, 
                    src.tlac, 
                    src.username, 
                    src.vers,     
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
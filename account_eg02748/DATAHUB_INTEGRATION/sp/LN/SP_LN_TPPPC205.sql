create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPPC205()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPPC205_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPPC205 as target using (SELECT * FROM (SELECT 
            strm.cact, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_cspa_ref_compnr, 
            strm.cprj_cstl_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cspa, 
            strm.cstl, 
            strm.deleted, 
            strm.emno, 
            strm.emno_ref_compnr, 
            strm.entu, 
            strm.frdt, 
            strm.hper, 
            strm.hyea, 
            strm.otbp, 
            strm.otbp_ref_compnr, 
            strm.peri, 
            strm.rgdt, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.user, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj,
            strm.user ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC205 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.user=src.user) OR (target.user IS NULL AND src.user IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.cact=src.cact, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_cspa_ref_compnr=src.cprj_cspa_ref_compnr, 
                    target.cprj_cstl_ref_compnr=src.cprj_cstl_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.cstl=src.cstl, 
                    target.deleted=src.deleted, 
                    target.emno=src.emno, 
                    target.emno_ref_compnr=src.emno_ref_compnr, 
                    target.entu=src.entu, 
                    target.frdt=src.frdt, 
                    target.hper=src.hper, 
                    target.hyea=src.hyea, 
                    target.otbp=src.otbp, 
                    target.otbp_ref_compnr=src.otbp_ref_compnr, 
                    target.peri=src.peri, 
                    target.rgdt=src.rgdt, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.user=src.user, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    cact, 
                    compnr, 
                    cpla, 
                    cprj, 
                    cprj_cpla_cact_ref_compnr, 
                    cprj_cspa_ref_compnr, 
                    cprj_cstl_ref_compnr, 
                    cprj_ref_compnr, 
                    cspa, 
                    cstl, 
                    deleted, 
                    emno, 
                    emno_ref_compnr, 
                    entu, 
                    frdt, 
                    hper, 
                    hyea, 
                    otbp, 
                    otbp_ref_compnr, 
                    peri, 
                    rgdt, 
                    sequencenumber, 
                    timestamp, 
                    user, 
                    username, 
                    year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.cact, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_cspa_ref_compnr, 
                    src.cprj_cstl_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cspa, 
                    src.cstl, 
                    src.deleted, 
                    src.emno, 
                    src.emno_ref_compnr, 
                    src.entu, 
                    src.frdt, 
                    src.hper, 
                    src.hyea, 
                    src.otbp, 
                    src.otbp_ref_compnr, 
                    src.peri, 
                    src.rgdt, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.user, 
                    src.username, 
                    src.year,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPPC205_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.cact, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_cspa_ref_compnr, 
            strm.cprj_cstl_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cspa, 
            strm.cstl, 
            strm.deleted, 
            strm.emno, 
            strm.emno_ref_compnr, 
            strm.entu, 
            strm.frdt, 
            strm.hper, 
            strm.hyea, 
            strm.otbp, 
            strm.otbp_ref_compnr, 
            strm.peri, 
            strm.rgdt, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.user, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj,
            strm.user ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPPC205 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.user=src.user) OR (target.user IS NULL AND src.user IS NULL)) 
                when matched then update set
                    target.cact=src.cact, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_cspa_ref_compnr=src.cprj_cspa_ref_compnr, 
                    target.cprj_cstl_ref_compnr=src.cprj_cstl_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cspa=src.cspa, 
                    target.cstl=src.cstl, 
                    target.deleted=src.deleted, 
                    target.emno=src.emno, 
                    target.emno_ref_compnr=src.emno_ref_compnr, 
                    target.entu=src.entu, 
                    target.frdt=src.frdt, 
                    target.hper=src.hper, 
                    target.hyea=src.hyea, 
                    target.otbp=src.otbp, 
                    target.otbp_ref_compnr=src.otbp_ref_compnr, 
                    target.peri=src.peri, 
                    target.rgdt=src.rgdt, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.user=src.user, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( cact, compnr, cpla, cprj, cprj_cpla_cact_ref_compnr, cprj_cspa_ref_compnr, cprj_cstl_ref_compnr, cprj_ref_compnr, cspa, cstl, deleted, emno, emno_ref_compnr, entu, frdt, hper, hyea, otbp, otbp_ref_compnr, peri, rgdt, sequencenumber, timestamp, user, username, year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.cact, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_cspa_ref_compnr, 
                    src.cprj_cstl_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cspa, 
                    src.cstl, 
                    src.deleted, 
                    src.emno, 
                    src.emno_ref_compnr, 
                    src.entu, 
                    src.frdt, 
                    src.hper, 
                    src.hyea, 
                    src.otbp, 
                    src.otbp_ref_compnr, 
                    src.peri, 
                    src.rgdt, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.user, 
                    src.username, 
                    src.year,     
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
create or replace procedure DATAHUB_INTEGRATION.SP_LN_TPPTC510()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TPPTC510_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TPPTC510 as target using (SELECT * FROM (SELECT 
            strm.ambu, 
            strm.ameq, 
            strm.amit, 
            strm.amsb, 
            strm.amsn, 
            strm.amts, 
            strm.cact, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cprj_vers_ref_compnr, 
            strm.deleted, 
            strm.rate_1, 
            strm.rate_2, 
            strm.rate_3, 
            strm.ratf_1, 
            strm.ratf_2, 
            strm.ratf_3, 
            strm.rdat, 
            strm.rgdt, 
            strm.sequencenumber, 
            strm.stat, 
            strm.stat_kw, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.vers, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj,
            strm.vers,
            strm.cact,
            strm.rgdt ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPTC510 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.vers=src.vers) OR (target.vers IS NULL AND src.vers IS NULL)) AND
            ((target.cact=src.cact) OR (target.cact IS NULL AND src.cact IS NULL)) AND
            ((target.rgdt=src.rgdt) OR (target.rgdt IS NULL AND src.rgdt IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ambu=src.ambu, 
                    target.ameq=src.ameq, 
                    target.amit=src.amit, 
                    target.amsb=src.amsb, 
                    target.amsn=src.amsn, 
                    target.amts=src.amts, 
                    target.cact=src.cact, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cprj_vers_ref_compnr=src.cprj_vers_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.rate_1=src.rate_1, 
                    target.rate_2=src.rate_2, 
                    target.rate_3=src.rate_3, 
                    target.ratf_1=src.ratf_1, 
                    target.ratf_2=src.ratf_2, 
                    target.ratf_3=src.ratf_3, 
                    target.rdat=src.rdat, 
                    target.rgdt=src.rgdt, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.vers=src.vers, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ambu, 
                    ameq, 
                    amit, 
                    amsb, 
                    amsn, 
                    amts, 
                    cact, 
                    ccur, 
                    ccur_ref_compnr, 
                    compnr, 
                    cpla, 
                    cprj, 
                    cprj_cpla_cact_ref_compnr, 
                    cprj_ref_compnr, 
                    cprj_vers_ref_compnr, 
                    deleted, 
                    rate_1, 
                    rate_2, 
                    rate_3, 
                    ratf_1, 
                    ratf_2, 
                    ratf_3, 
                    rdat, 
                    rgdt, 
                    sequencenumber, 
                    stat, 
                    stat_kw, 
                    timestamp, 
                    txta, 
                    txta_ref_compnr, 
                    username, 
                    vers, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ambu, 
                    src.ameq, 
                    src.amit, 
                    src.amsb, 
                    src.amsn, 
                    src.amts, 
                    src.cact, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cprj_vers_ref_compnr, 
                    src.deleted, 
                    src.rate_1, 
                    src.rate_2, 
                    src.rate_3, 
                    src.ratf_1, 
                    src.ratf_2, 
                    src.ratf_3, 
                    src.rdat, 
                    src.rgdt, 
                    src.sequencenumber, 
                    src.stat, 
                    src.stat_kw, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
                    src.username, 
                    src.vers,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TPPTC510_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ambu, 
            strm.ameq, 
            strm.amit, 
            strm.amsb, 
            strm.amsn, 
            strm.amts, 
            strm.cact, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cpla, 
            strm.cprj, 
            strm.cprj_cpla_cact_ref_compnr, 
            strm.cprj_ref_compnr, 
            strm.cprj_vers_ref_compnr, 
            strm.deleted, 
            strm.rate_1, 
            strm.rate_2, 
            strm.rate_3, 
            strm.ratf_1, 
            strm.ratf_2, 
            strm.ratf_3, 
            strm.rdat, 
            strm.rgdt, 
            strm.sequencenumber, 
            strm.stat, 
            strm.stat_kw, 
            strm.timestamp, 
            strm.txta, 
            strm.txta_ref_compnr, 
            strm.username, 
            strm.vers, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cprj,
            strm.vers,
            strm.cact,
            strm.rgdt ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TPPTC510 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cprj=src.cprj) OR (target.cprj IS NULL AND src.cprj IS NULL)) AND
            ((target.vers=src.vers) OR (target.vers IS NULL AND src.vers IS NULL)) AND
            ((target.cact=src.cact) OR (target.cact IS NULL AND src.cact IS NULL)) AND
            ((target.rgdt=src.rgdt) OR (target.rgdt IS NULL AND src.rgdt IS NULL)) 
                when matched then update set
                    target.ambu=src.ambu, 
                    target.ameq=src.ameq, 
                    target.amit=src.amit, 
                    target.amsb=src.amsb, 
                    target.amsn=src.amsn, 
                    target.amts=src.amts, 
                    target.cact=src.cact, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpla=src.cpla, 
                    target.cprj=src.cprj, 
                    target.cprj_cpla_cact_ref_compnr=src.cprj_cpla_cact_ref_compnr, 
                    target.cprj_ref_compnr=src.cprj_ref_compnr, 
                    target.cprj_vers_ref_compnr=src.cprj_vers_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.rate_1=src.rate_1, 
                    target.rate_2=src.rate_2, 
                    target.rate_3=src.rate_3, 
                    target.ratf_1=src.ratf_1, 
                    target.ratf_2=src.ratf_2, 
                    target.ratf_3=src.ratf_3, 
                    target.rdat=src.rdat, 
                    target.rgdt=src.rgdt, 
                    target.sequencenumber=src.sequencenumber, 
                    target.stat=src.stat, 
                    target.stat_kw=src.stat_kw, 
                    target.timestamp=src.timestamp, 
                    target.txta=src.txta, 
                    target.txta_ref_compnr=src.txta_ref_compnr, 
                    target.username=src.username, 
                    target.vers=src.vers, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ambu, ameq, amit, amsb, amsn, amts, cact, ccur, ccur_ref_compnr, compnr, cpla, cprj, cprj_cpla_cact_ref_compnr, cprj_ref_compnr, cprj_vers_ref_compnr, deleted, rate_1, rate_2, rate_3, ratf_1, ratf_2, ratf_3, rdat, rgdt, sequencenumber, stat, stat_kw, timestamp, txta, txta_ref_compnr, username, vers, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ambu, 
                    src.ameq, 
                    src.amit, 
                    src.amsb, 
                    src.amsn, 
                    src.amts, 
                    src.cact, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cpla, 
                    src.cprj, 
                    src.cprj_cpla_cact_ref_compnr, 
                    src.cprj_ref_compnr, 
                    src.cprj_vers_ref_compnr, 
                    src.deleted, 
                    src.rate_1, 
                    src.rate_2, 
                    src.rate_3, 
                    src.ratf_1, 
                    src.ratf_2, 
                    src.ratf_3, 
                    src.rdat, 
                    src.rgdt, 
                    src.sequencenumber, 
                    src.stat, 
                    src.stat_kw, 
                    src.timestamp, 
                    src.txta, 
                    src.txta_ref_compnr, 
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
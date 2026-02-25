create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFGLD206()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFGLD206_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFGLD206 as target using (SELECT * FROM (SELECT 
            strm.bpid, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cono, 
            strm.deleted, 
            strm.dim1, 
            strm.dim2, 
            strm.dim3, 
            strm.dim4, 
            strm.dim5, 
            strm.dim6, 
            strm.dim7, 
            strm.dim8, 
            strm.dim9, 
            strm.dims, 
            strm.dimx_sgm1, 
            strm.dimx_sgm2, 
            strm.dm10, 
            strm.dm11, 
            strm.dm12, 
            strm.duac, 
            strm.duac_kw, 
            strm.fobh_1, 
            strm.fobh_2, 
            strm.fobh_3, 
            strm.fobh_dtwc, 
            strm.fobh_rfrc, 
            strm.ftob, 
            strm.leac, 
            strm.nob1, 
            strm.nob2, 
            strm.nobh_1, 
            strm.nobh_2, 
            strm.nobh_3, 
            strm.nobh_dtwc, 
            strm.nobh_rfrc, 
            strm.ntob, 
            strm.olap, 
            strm.qob1, 
            strm.qob2, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cono,
            strm.year,
            strm.dim1,
            strm.dim2,
            strm.dim3,
            strm.dim4,
            strm.dim5,
            strm.dims,
            strm.leac,
            strm.ccur,
            strm.duac,
            strm.bpid ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD206 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cono=src.cono) OR (target.cono IS NULL AND src.cono IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.dim1=src.dim1) OR (target.dim1 IS NULL AND src.dim1 IS NULL)) AND
            ((target.dim2=src.dim2) OR (target.dim2 IS NULL AND src.dim2 IS NULL)) AND
            ((target.dim3=src.dim3) OR (target.dim3 IS NULL AND src.dim3 IS NULL)) AND
            ((target.dim4=src.dim4) OR (target.dim4 IS NULL AND src.dim4 IS NULL)) AND
            ((target.dim5=src.dim5) OR (target.dim5 IS NULL AND src.dim5 IS NULL)) AND
            ((target.dims=src.dims) OR (target.dims IS NULL AND src.dims IS NULL)) AND
            ((target.leac=src.leac) OR (target.leac IS NULL AND src.leac IS NULL)) AND
            ((target.ccur=src.ccur) OR (target.ccur IS NULL AND src.ccur IS NULL)) AND
            ((target.duac=src.duac) OR (target.duac IS NULL AND src.duac IS NULL)) AND
            ((target.bpid=src.bpid) OR (target.bpid IS NULL AND src.bpid IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.bpid=src.bpid, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cono=src.cono, 
                    target.deleted=src.deleted, 
                    target.dim1=src.dim1, 
                    target.dim2=src.dim2, 
                    target.dim3=src.dim3, 
                    target.dim4=src.dim4, 
                    target.dim5=src.dim5, 
                    target.dim6=src.dim6, 
                    target.dim7=src.dim7, 
                    target.dim8=src.dim8, 
                    target.dim9=src.dim9, 
                    target.dims=src.dims, 
                    target.dimx_sgm1=src.dimx_sgm1, 
                    target.dimx_sgm2=src.dimx_sgm2, 
                    target.dm10=src.dm10, 
                    target.dm11=src.dm11, 
                    target.dm12=src.dm12, 
                    target.duac=src.duac, 
                    target.duac_kw=src.duac_kw, 
                    target.fobh_1=src.fobh_1, 
                    target.fobh_2=src.fobh_2, 
                    target.fobh_3=src.fobh_3, 
                    target.fobh_dtwc=src.fobh_dtwc, 
                    target.fobh_rfrc=src.fobh_rfrc, 
                    target.ftob=src.ftob, 
                    target.leac=src.leac, 
                    target.nob1=src.nob1, 
                    target.nob2=src.nob2, 
                    target.nobh_1=src.nobh_1, 
                    target.nobh_2=src.nobh_2, 
                    target.nobh_3=src.nobh_3, 
                    target.nobh_dtwc=src.nobh_dtwc, 
                    target.nobh_rfrc=src.nobh_rfrc, 
                    target.ntob=src.ntob, 
                    target.olap=src.olap, 
                    target.qob1=src.qob1, 
                    target.qob2=src.qob2, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    bpid, 
                    ccur, 
                    ccur_ref_compnr, 
                    compnr, 
                    cono, 
                    deleted, 
                    dim1, 
                    dim2, 
                    dim3, 
                    dim4, 
                    dim5, 
                    dim6, 
                    dim7, 
                    dim8, 
                    dim9, 
                    dims, 
                    dimx_sgm1, 
                    dimx_sgm2, 
                    dm10, 
                    dm11, 
                    dm12, 
                    duac, 
                    duac_kw, 
                    fobh_1, 
                    fobh_2, 
                    fobh_3, 
                    fobh_dtwc, 
                    fobh_rfrc, 
                    ftob, 
                    leac, 
                    nob1, 
                    nob2, 
                    nobh_1, 
                    nobh_2, 
                    nobh_3, 
                    nobh_dtwc, 
                    nobh_rfrc, 
                    ntob, 
                    olap, 
                    qob1, 
                    qob2, 
                    sequencenumber, 
                    timestamp, 
                    username, 
                    year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.bpid, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cono, 
                    src.deleted, 
                    src.dim1, 
                    src.dim2, 
                    src.dim3, 
                    src.dim4, 
                    src.dim5, 
                    src.dim6, 
                    src.dim7, 
                    src.dim8, 
                    src.dim9, 
                    src.dims, 
                    src.dimx_sgm1, 
                    src.dimx_sgm2, 
                    src.dm10, 
                    src.dm11, 
                    src.dm12, 
                    src.duac, 
                    src.duac_kw, 
                    src.fobh_1, 
                    src.fobh_2, 
                    src.fobh_3, 
                    src.fobh_dtwc, 
                    src.fobh_rfrc, 
                    src.ftob, 
                    src.leac, 
                    src.nob1, 
                    src.nob2, 
                    src.nobh_1, 
                    src.nobh_2, 
                    src.nobh_3, 
                    src.nobh_dtwc, 
                    src.nobh_rfrc, 
                    src.ntob, 
                    src.olap, 
                    src.qob1, 
                    src.qob2, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.username, 
                    src.year,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFGLD206_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.bpid, 
            strm.ccur, 
            strm.ccur_ref_compnr, 
            strm.compnr, 
            strm.cono, 
            strm.deleted, 
            strm.dim1, 
            strm.dim2, 
            strm.dim3, 
            strm.dim4, 
            strm.dim5, 
            strm.dim6, 
            strm.dim7, 
            strm.dim8, 
            strm.dim9, 
            strm.dims, 
            strm.dimx_sgm1, 
            strm.dimx_sgm2, 
            strm.dm10, 
            strm.dm11, 
            strm.dm12, 
            strm.duac, 
            strm.duac_kw, 
            strm.fobh_1, 
            strm.fobh_2, 
            strm.fobh_3, 
            strm.fobh_dtwc, 
            strm.fobh_rfrc, 
            strm.ftob, 
            strm.leac, 
            strm.nob1, 
            strm.nob2, 
            strm.nobh_1, 
            strm.nobh_2, 
            strm.nobh_3, 
            strm.nobh_dtwc, 
            strm.nobh_rfrc, 
            strm.ntob, 
            strm.olap, 
            strm.qob1, 
            strm.qob2, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.cono,
            strm.year,
            strm.dim1,
            strm.dim2,
            strm.dim3,
            strm.dim4,
            strm.dim5,
            strm.dims,
            strm.leac,
            strm.ccur,
            strm.duac,
            strm.bpid ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD206 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.cono=src.cono) OR (target.cono IS NULL AND src.cono IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.dim1=src.dim1) OR (target.dim1 IS NULL AND src.dim1 IS NULL)) AND
            ((target.dim2=src.dim2) OR (target.dim2 IS NULL AND src.dim2 IS NULL)) AND
            ((target.dim3=src.dim3) OR (target.dim3 IS NULL AND src.dim3 IS NULL)) AND
            ((target.dim4=src.dim4) OR (target.dim4 IS NULL AND src.dim4 IS NULL)) AND
            ((target.dim5=src.dim5) OR (target.dim5 IS NULL AND src.dim5 IS NULL)) AND
            ((target.dims=src.dims) OR (target.dims IS NULL AND src.dims IS NULL)) AND
            ((target.leac=src.leac) OR (target.leac IS NULL AND src.leac IS NULL)) AND
            ((target.ccur=src.ccur) OR (target.ccur IS NULL AND src.ccur IS NULL)) AND
            ((target.duac=src.duac) OR (target.duac IS NULL AND src.duac IS NULL)) AND
            ((target.bpid=src.bpid) OR (target.bpid IS NULL AND src.bpid IS NULL)) 
                when matched then update set
                    target.bpid=src.bpid, 
                    target.ccur=src.ccur, 
                    target.ccur_ref_compnr=src.ccur_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cono=src.cono, 
                    target.deleted=src.deleted, 
                    target.dim1=src.dim1, 
                    target.dim2=src.dim2, 
                    target.dim3=src.dim3, 
                    target.dim4=src.dim4, 
                    target.dim5=src.dim5, 
                    target.dim6=src.dim6, 
                    target.dim7=src.dim7, 
                    target.dim8=src.dim8, 
                    target.dim9=src.dim9, 
                    target.dims=src.dims, 
                    target.dimx_sgm1=src.dimx_sgm1, 
                    target.dimx_sgm2=src.dimx_sgm2, 
                    target.dm10=src.dm10, 
                    target.dm11=src.dm11, 
                    target.dm12=src.dm12, 
                    target.duac=src.duac, 
                    target.duac_kw=src.duac_kw, 
                    target.fobh_1=src.fobh_1, 
                    target.fobh_2=src.fobh_2, 
                    target.fobh_3=src.fobh_3, 
                    target.fobh_dtwc=src.fobh_dtwc, 
                    target.fobh_rfrc=src.fobh_rfrc, 
                    target.ftob=src.ftob, 
                    target.leac=src.leac, 
                    target.nob1=src.nob1, 
                    target.nob2=src.nob2, 
                    target.nobh_1=src.nobh_1, 
                    target.nobh_2=src.nobh_2, 
                    target.nobh_3=src.nobh_3, 
                    target.nobh_dtwc=src.nobh_dtwc, 
                    target.nobh_rfrc=src.nobh_rfrc, 
                    target.ntob=src.ntob, 
                    target.olap=src.olap, 
                    target.qob1=src.qob1, 
                    target.qob2=src.qob2, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( bpid, ccur, ccur_ref_compnr, compnr, cono, deleted, dim1, dim2, dim3, dim4, dim5, dim6, dim7, dim8, dim9, dims, dimx_sgm1, dimx_sgm2, dm10, dm11, dm12, duac, duac_kw, fobh_1, fobh_2, fobh_3, fobh_dtwc, fobh_rfrc, ftob, leac, nob1, nob2, nobh_1, nobh_2, nobh_3, nobh_dtwc, nobh_rfrc, ntob, olap, qob1, qob2, sequencenumber, timestamp, username, year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.bpid, 
                    src.ccur, 
                    src.ccur_ref_compnr, 
                    src.compnr, 
                    src.cono, 
                    src.deleted, 
                    src.dim1, 
                    src.dim2, 
                    src.dim3, 
                    src.dim4, 
                    src.dim5, 
                    src.dim6, 
                    src.dim7, 
                    src.dim8, 
                    src.dim9, 
                    src.dims, 
                    src.dimx_sgm1, 
                    src.dimx_sgm2, 
                    src.dm10, 
                    src.dm11, 
                    src.dm12, 
                    src.duac, 
                    src.duac_kw, 
                    src.fobh_1, 
                    src.fobh_2, 
                    src.fobh_3, 
                    src.fobh_dtwc, 
                    src.fobh_rfrc, 
                    src.ftob, 
                    src.leac, 
                    src.nob1, 
                    src.nob2, 
                    src.nobh_1, 
                    src.nobh_2, 
                    src.nobh_3, 
                    src.nobh_dtwc, 
                    src.nobh_rfrc, 
                    src.ntob, 
                    src.olap, 
                    src.qob1, 
                    src.qob2, 
                    src.sequencenumber, 
                    src.timestamp, 
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
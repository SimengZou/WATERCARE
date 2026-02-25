create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFFAM807()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFFAM807_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFFAM807 as target using (SELECT * FROM (SELECT 
            strm.acct, 
            strm.acqa_1, 
            strm.acqa_2, 
            strm.acqa_3, 
            strm.adja_1, 
            strm.adja_2, 
            strm.adja_3, 
            strm.book, 
            strm.book_ref_compnr, 
            strm.comp, 
            strm.compnr, 
            strm.deleted, 
            strm.depr_1, 
            strm.depr_2, 
            strm.depr_3, 
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
            strm.disa_1, 
            strm.disa_2, 
            strm.disa_3, 
            strm.dm10, 
            strm.dm11, 
            strm.dm12, 
            strm.idtc, 
            strm.lkey, 
            strm.proa_1, 
            strm.proa_2, 
            strm.proa_3, 
            strm.prod, 
            strm.rcst_1, 
            strm.rcst_2, 
            strm.rcst_3, 
            strm.rdpr_1, 
            strm.rdpr_2, 
            strm.rdpr_3, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.traa_1, 
            strm.traa_2, 
            strm.traa_3, 
            strm.trct_1, 
            strm.trct_2, 
            strm.trct_3, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.comp,
            strm.acct,
            strm.dim1,
            strm.dim2,
            strm.dim3,
            strm.dim4,
            strm.dim5,
            strm.dims,
            strm.book,
            strm.lkey,
            strm.idtc,
            strm.year,
            strm.prod ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM807 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.comp=src.comp) OR (target.comp IS NULL AND src.comp IS NULL)) AND
            ((target.acct=src.acct) OR (target.acct IS NULL AND src.acct IS NULL)) AND
            ((target.dim1=src.dim1) OR (target.dim1 IS NULL AND src.dim1 IS NULL)) AND
            ((target.dim2=src.dim2) OR (target.dim2 IS NULL AND src.dim2 IS NULL)) AND
            ((target.dim3=src.dim3) OR (target.dim3 IS NULL AND src.dim3 IS NULL)) AND
            ((target.dim4=src.dim4) OR (target.dim4 IS NULL AND src.dim4 IS NULL)) AND
            ((target.dim5=src.dim5) OR (target.dim5 IS NULL AND src.dim5 IS NULL)) AND
            ((target.dims=src.dims) OR (target.dims IS NULL AND src.dims IS NULL)) AND
            ((target.book=src.book) OR (target.book IS NULL AND src.book IS NULL)) AND
            ((target.lkey=src.lkey) OR (target.lkey IS NULL AND src.lkey IS NULL)) AND
            ((target.idtc=src.idtc) OR (target.idtc IS NULL AND src.idtc IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.prod=src.prod) OR (target.prod IS NULL AND src.prod IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.acct=src.acct, 
                    target.acqa_1=src.acqa_1, 
                    target.acqa_2=src.acqa_2, 
                    target.acqa_3=src.acqa_3, 
                    target.adja_1=src.adja_1, 
                    target.adja_2=src.adja_2, 
                    target.adja_3=src.adja_3, 
                    target.book=src.book, 
                    target.book_ref_compnr=src.book_ref_compnr, 
                    target.comp=src.comp, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.depr_1=src.depr_1, 
                    target.depr_2=src.depr_2, 
                    target.depr_3=src.depr_3, 
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
                    target.disa_1=src.disa_1, 
                    target.disa_2=src.disa_2, 
                    target.disa_3=src.disa_3, 
                    target.dm10=src.dm10, 
                    target.dm11=src.dm11, 
                    target.dm12=src.dm12, 
                    target.idtc=src.idtc, 
                    target.lkey=src.lkey, 
                    target.proa_1=src.proa_1, 
                    target.proa_2=src.proa_2, 
                    target.proa_3=src.proa_3, 
                    target.prod=src.prod, 
                    target.rcst_1=src.rcst_1, 
                    target.rcst_2=src.rcst_2, 
                    target.rcst_3=src.rcst_3, 
                    target.rdpr_1=src.rdpr_1, 
                    target.rdpr_2=src.rdpr_2, 
                    target.rdpr_3=src.rdpr_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.traa_1=src.traa_1, 
                    target.traa_2=src.traa_2, 
                    target.traa_3=src.traa_3, 
                    target.trct_1=src.trct_1, 
                    target.trct_2=src.trct_2, 
                    target.trct_3=src.trct_3, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    acct, 
                    acqa_1, 
                    acqa_2, 
                    acqa_3, 
                    adja_1, 
                    adja_2, 
                    adja_3, 
                    book, 
                    book_ref_compnr, 
                    comp, 
                    compnr, 
                    deleted, 
                    depr_1, 
                    depr_2, 
                    depr_3, 
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
                    disa_1, 
                    disa_2, 
                    disa_3, 
                    dm10, 
                    dm11, 
                    dm12, 
                    idtc, 
                    lkey, 
                    proa_1, 
                    proa_2, 
                    proa_3, 
                    prod, 
                    rcst_1, 
                    rcst_2, 
                    rcst_3, 
                    rdpr_1, 
                    rdpr_2, 
                    rdpr_3, 
                    sequencenumber, 
                    timestamp, 
                    traa_1, 
                    traa_2, 
                    traa_3, 
                    trct_1, 
                    trct_2, 
                    trct_3, 
                    username, 
                    year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.acct, 
                    src.acqa_1, 
                    src.acqa_2, 
                    src.acqa_3, 
                    src.adja_1, 
                    src.adja_2, 
                    src.adja_3, 
                    src.book, 
                    src.book_ref_compnr, 
                    src.comp, 
                    src.compnr, 
                    src.deleted, 
                    src.depr_1, 
                    src.depr_2, 
                    src.depr_3, 
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
                    src.disa_1, 
                    src.disa_2, 
                    src.disa_3, 
                    src.dm10, 
                    src.dm11, 
                    src.dm12, 
                    src.idtc, 
                    src.lkey, 
                    src.proa_1, 
                    src.proa_2, 
                    src.proa_3, 
                    src.prod, 
                    src.rcst_1, 
                    src.rcst_2, 
                    src.rcst_3, 
                    src.rdpr_1, 
                    src.rdpr_2, 
                    src.rdpr_3, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.traa_1, 
                    src.traa_2, 
                    src.traa_3, 
                    src.trct_1, 
                    src.trct_2, 
                    src.trct_3, 
                    src.username, 
                    src.year,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFFAM807_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.acct, 
            strm.acqa_1, 
            strm.acqa_2, 
            strm.acqa_3, 
            strm.adja_1, 
            strm.adja_2, 
            strm.adja_3, 
            strm.book, 
            strm.book_ref_compnr, 
            strm.comp, 
            strm.compnr, 
            strm.deleted, 
            strm.depr_1, 
            strm.depr_2, 
            strm.depr_3, 
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
            strm.disa_1, 
            strm.disa_2, 
            strm.disa_3, 
            strm.dm10, 
            strm.dm11, 
            strm.dm12, 
            strm.idtc, 
            strm.lkey, 
            strm.proa_1, 
            strm.proa_2, 
            strm.proa_3, 
            strm.prod, 
            strm.rcst_1, 
            strm.rcst_2, 
            strm.rcst_3, 
            strm.rdpr_1, 
            strm.rdpr_2, 
            strm.rdpr_3, 
            strm.sequencenumber, 
            strm.timestamp, 
            strm.traa_1, 
            strm.traa_2, 
            strm.traa_3, 
            strm.trct_1, 
            strm.trct_2, 
            strm.trct_3, 
            strm.username, 
            strm.year, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.comp,
            strm.acct,
            strm.dim1,
            strm.dim2,
            strm.dim3,
            strm.dim4,
            strm.dim5,
            strm.dims,
            strm.book,
            strm.lkey,
            strm.idtc,
            strm.year,
            strm.prod ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFAM807 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.comp=src.comp) OR (target.comp IS NULL AND src.comp IS NULL)) AND
            ((target.acct=src.acct) OR (target.acct IS NULL AND src.acct IS NULL)) AND
            ((target.dim1=src.dim1) OR (target.dim1 IS NULL AND src.dim1 IS NULL)) AND
            ((target.dim2=src.dim2) OR (target.dim2 IS NULL AND src.dim2 IS NULL)) AND
            ((target.dim3=src.dim3) OR (target.dim3 IS NULL AND src.dim3 IS NULL)) AND
            ((target.dim4=src.dim4) OR (target.dim4 IS NULL AND src.dim4 IS NULL)) AND
            ((target.dim5=src.dim5) OR (target.dim5 IS NULL AND src.dim5 IS NULL)) AND
            ((target.dims=src.dims) OR (target.dims IS NULL AND src.dims IS NULL)) AND
            ((target.book=src.book) OR (target.book IS NULL AND src.book IS NULL)) AND
            ((target.lkey=src.lkey) OR (target.lkey IS NULL AND src.lkey IS NULL)) AND
            ((target.idtc=src.idtc) OR (target.idtc IS NULL AND src.idtc IS NULL)) AND
            ((target.year=src.year) OR (target.year IS NULL AND src.year IS NULL)) AND
            ((target.prod=src.prod) OR (target.prod IS NULL AND src.prod IS NULL)) 
                when matched then update set
                    target.acct=src.acct, 
                    target.acqa_1=src.acqa_1, 
                    target.acqa_2=src.acqa_2, 
                    target.acqa_3=src.acqa_3, 
                    target.adja_1=src.adja_1, 
                    target.adja_2=src.adja_2, 
                    target.adja_3=src.adja_3, 
                    target.book=src.book, 
                    target.book_ref_compnr=src.book_ref_compnr, 
                    target.comp=src.comp, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.depr_1=src.depr_1, 
                    target.depr_2=src.depr_2, 
                    target.depr_3=src.depr_3, 
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
                    target.disa_1=src.disa_1, 
                    target.disa_2=src.disa_2, 
                    target.disa_3=src.disa_3, 
                    target.dm10=src.dm10, 
                    target.dm11=src.dm11, 
                    target.dm12=src.dm12, 
                    target.idtc=src.idtc, 
                    target.lkey=src.lkey, 
                    target.proa_1=src.proa_1, 
                    target.proa_2=src.proa_2, 
                    target.proa_3=src.proa_3, 
                    target.prod=src.prod, 
                    target.rcst_1=src.rcst_1, 
                    target.rcst_2=src.rcst_2, 
                    target.rcst_3=src.rcst_3, 
                    target.rdpr_1=src.rdpr_1, 
                    target.rdpr_2=src.rdpr_2, 
                    target.rdpr_3=src.rdpr_3, 
                    target.sequencenumber=src.sequencenumber, 
                    target.timestamp=src.timestamp, 
                    target.traa_1=src.traa_1, 
                    target.traa_2=src.traa_2, 
                    target.traa_3=src.traa_3, 
                    target.trct_1=src.trct_1, 
                    target.trct_2=src.trct_2, 
                    target.trct_3=src.trct_3, 
                    target.username=src.username, 
                    target.year=src.year, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( acct, acqa_1, acqa_2, acqa_3, adja_1, adja_2, adja_3, book, book_ref_compnr, comp, compnr, deleted, depr_1, depr_2, depr_3, dim1, dim2, dim3, dim4, dim5, dim6, dim7, dim8, dim9, dims, disa_1, disa_2, disa_3, dm10, dm11, dm12, idtc, lkey, proa_1, proa_2, proa_3, prod, rcst_1, rcst_2, rcst_3, rdpr_1, rdpr_2, rdpr_3, sequencenumber, timestamp, traa_1, traa_2, traa_3, trct_1, trct_2, trct_3, username, year, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.acct, 
                    src.acqa_1, 
                    src.acqa_2, 
                    src.acqa_3, 
                    src.adja_1, 
                    src.adja_2, 
                    src.adja_3, 
                    src.book, 
                    src.book_ref_compnr, 
                    src.comp, 
                    src.compnr, 
                    src.deleted, 
                    src.depr_1, 
                    src.depr_2, 
                    src.depr_3, 
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
                    src.disa_1, 
                    src.disa_2, 
                    src.disa_3, 
                    src.dm10, 
                    src.dm11, 
                    src.dm12, 
                    src.idtc, 
                    src.lkey, 
                    src.proa_1, 
                    src.proa_2, 
                    src.proa_3, 
                    src.prod, 
                    src.rcst_1, 
                    src.rcst_2, 
                    src.rcst_3, 
                    src.rdpr_1, 
                    src.rdpr_2, 
                    src.rdpr_3, 
                    src.sequencenumber, 
                    src.timestamp, 
                    src.traa_1, 
                    src.traa_2, 
                    src.traa_3, 
                    src.trct_1, 
                    src.trct_2, 
                    src.trct_3, 
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
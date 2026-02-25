create or replace procedure DATAHUB_INTEGRATION.SP_LN_TFFBS003()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TFFBS003_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TFFBS003 as target using (SELECT * FROM (SELECT 
            strm.ad10, 
            strm.ad10_kw, 
            strm.ad11, 
            strm.ad11_kw, 
            strm.ad12, 
            strm.ad12_kw, 
            strm.adt1, 
            strm.adt1_kw, 
            strm.adt2, 
            strm.adt2_kw, 
            strm.adt3, 
            strm.adt3_kw, 
            strm.adt4, 
            strm.adt4_kw, 
            strm.adt5, 
            strm.adt5_kw, 
            strm.adt6, 
            strm.adt6_kw, 
            strm.adt7, 
            strm.adt7_kw, 
            strm.adt8, 
            strm.adt8_kw, 
            strm.adt9, 
            strm.adt9_kw, 
            strm.aqu1, 
            strm.aqu1_kw, 
            strm.aqu2, 
            strm.aqu2_kw, 
            strm.budg, 
            strm.budm, 
            strm.budm_kw, 
            strm.compnr, 
            strm.deleted, 
            strm.desc, 
            strm.llac, 
            strm.llac_kw, 
            strm.nbpr, 
            strm.sdbu, 
            strm.sdbu_kw, 
            strm.sequencenumber, 
            strm.text, 
            strm.text_ref_compnr, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.budg ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFBS003 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.budg=src.budg) OR (target.budg IS NULL AND src.budg IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ad10=src.ad10, 
                    target.ad10_kw=src.ad10_kw, 
                    target.ad11=src.ad11, 
                    target.ad11_kw=src.ad11_kw, 
                    target.ad12=src.ad12, 
                    target.ad12_kw=src.ad12_kw, 
                    target.adt1=src.adt1, 
                    target.adt1_kw=src.adt1_kw, 
                    target.adt2=src.adt2, 
                    target.adt2_kw=src.adt2_kw, 
                    target.adt3=src.adt3, 
                    target.adt3_kw=src.adt3_kw, 
                    target.adt4=src.adt4, 
                    target.adt4_kw=src.adt4_kw, 
                    target.adt5=src.adt5, 
                    target.adt5_kw=src.adt5_kw, 
                    target.adt6=src.adt6, 
                    target.adt6_kw=src.adt6_kw, 
                    target.adt7=src.adt7, 
                    target.adt7_kw=src.adt7_kw, 
                    target.adt8=src.adt8, 
                    target.adt8_kw=src.adt8_kw, 
                    target.adt9=src.adt9, 
                    target.adt9_kw=src.adt9_kw, 
                    target.aqu1=src.aqu1, 
                    target.aqu1_kw=src.aqu1_kw, 
                    target.aqu2=src.aqu2, 
                    target.aqu2_kw=src.aqu2_kw, 
                    target.budg=src.budg, 
                    target.budm=src.budm, 
                    target.budm_kw=src.budm_kw, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.llac=src.llac, 
                    target.llac_kw=src.llac_kw, 
                    target.nbpr=src.nbpr, 
                    target.sdbu=src.sdbu, 
                    target.sdbu_kw=src.sdbu_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.text=src.text, 
                    target.text_ref_compnr=src.text_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ad10, 
                    ad10_kw, 
                    ad11, 
                    ad11_kw, 
                    ad12, 
                    ad12_kw, 
                    adt1, 
                    adt1_kw, 
                    adt2, 
                    adt2_kw, 
                    adt3, 
                    adt3_kw, 
                    adt4, 
                    adt4_kw, 
                    adt5, 
                    adt5_kw, 
                    adt6, 
                    adt6_kw, 
                    adt7, 
                    adt7_kw, 
                    adt8, 
                    adt8_kw, 
                    adt9, 
                    adt9_kw, 
                    aqu1, 
                    aqu1_kw, 
                    aqu2, 
                    aqu2_kw, 
                    budg, 
                    budm, 
                    budm_kw, 
                    compnr, 
                    deleted, 
                    desc, 
                    llac, 
                    llac_kw, 
                    nbpr, 
                    sdbu, 
                    sdbu_kw, 
                    sequencenumber, 
                    text, 
                    text_ref_compnr, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ad10, 
                    src.ad10_kw, 
                    src.ad11, 
                    src.ad11_kw, 
                    src.ad12, 
                    src.ad12_kw, 
                    src.adt1, 
                    src.adt1_kw, 
                    src.adt2, 
                    src.adt2_kw, 
                    src.adt3, 
                    src.adt3_kw, 
                    src.adt4, 
                    src.adt4_kw, 
                    src.adt5, 
                    src.adt5_kw, 
                    src.adt6, 
                    src.adt6_kw, 
                    src.adt7, 
                    src.adt7_kw, 
                    src.adt8, 
                    src.adt8_kw, 
                    src.adt9, 
                    src.adt9_kw, 
                    src.aqu1, 
                    src.aqu1_kw, 
                    src.aqu2, 
                    src.aqu2_kw, 
                    src.budg, 
                    src.budm, 
                    src.budm_kw, 
                    src.compnr, 
                    src.deleted, 
                    src.desc, 
                    src.llac, 
                    src.llac_kw, 
                    src.nbpr, 
                    src.sdbu, 
                    src.sdbu_kw, 
                    src.sequencenumber, 
                    src.text, 
                    src.text_ref_compnr, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TFFBS003_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ad10, 
            strm.ad10_kw, 
            strm.ad11, 
            strm.ad11_kw, 
            strm.ad12, 
            strm.ad12_kw, 
            strm.adt1, 
            strm.adt1_kw, 
            strm.adt2, 
            strm.adt2_kw, 
            strm.adt3, 
            strm.adt3_kw, 
            strm.adt4, 
            strm.adt4_kw, 
            strm.adt5, 
            strm.adt5_kw, 
            strm.adt6, 
            strm.adt6_kw, 
            strm.adt7, 
            strm.adt7_kw, 
            strm.adt8, 
            strm.adt8_kw, 
            strm.adt9, 
            strm.adt9_kw, 
            strm.aqu1, 
            strm.aqu1_kw, 
            strm.aqu2, 
            strm.aqu2_kw, 
            strm.budg, 
            strm.budm, 
            strm.budm_kw, 
            strm.compnr, 
            strm.deleted, 
            strm.desc, 
            strm.llac, 
            strm.llac_kw, 
            strm.nbpr, 
            strm.sdbu, 
            strm.sdbu_kw, 
            strm.sequencenumber, 
            strm.text, 
            strm.text_ref_compnr, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.budg ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TFFBS003 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.budg=src.budg) OR (target.budg IS NULL AND src.budg IS NULL)) 
                when matched then update set
                    target.ad10=src.ad10, 
                    target.ad10_kw=src.ad10_kw, 
                    target.ad11=src.ad11, 
                    target.ad11_kw=src.ad11_kw, 
                    target.ad12=src.ad12, 
                    target.ad12_kw=src.ad12_kw, 
                    target.adt1=src.adt1, 
                    target.adt1_kw=src.adt1_kw, 
                    target.adt2=src.adt2, 
                    target.adt2_kw=src.adt2_kw, 
                    target.adt3=src.adt3, 
                    target.adt3_kw=src.adt3_kw, 
                    target.adt4=src.adt4, 
                    target.adt4_kw=src.adt4_kw, 
                    target.adt5=src.adt5, 
                    target.adt5_kw=src.adt5_kw, 
                    target.adt6=src.adt6, 
                    target.adt6_kw=src.adt6_kw, 
                    target.adt7=src.adt7, 
                    target.adt7_kw=src.adt7_kw, 
                    target.adt8=src.adt8, 
                    target.adt8_kw=src.adt8_kw, 
                    target.adt9=src.adt9, 
                    target.adt9_kw=src.adt9_kw, 
                    target.aqu1=src.aqu1, 
                    target.aqu1_kw=src.aqu1_kw, 
                    target.aqu2=src.aqu2, 
                    target.aqu2_kw=src.aqu2_kw, 
                    target.budg=src.budg, 
                    target.budm=src.budm, 
                    target.budm_kw=src.budm_kw, 
                    target.compnr=src.compnr, 
                    target.deleted=src.deleted, 
                    target.desc=src.desc, 
                    target.llac=src.llac, 
                    target.llac_kw=src.llac_kw, 
                    target.nbpr=src.nbpr, 
                    target.sdbu=src.sdbu, 
                    target.sdbu_kw=src.sdbu_kw, 
                    target.sequencenumber=src.sequencenumber, 
                    target.text=src.text, 
                    target.text_ref_compnr=src.text_ref_compnr, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ad10, ad10_kw, ad11, ad11_kw, ad12, ad12_kw, adt1, adt1_kw, adt2, adt2_kw, adt3, adt3_kw, adt4, adt4_kw, adt5, adt5_kw, adt6, adt6_kw, adt7, adt7_kw, adt8, adt8_kw, adt9, adt9_kw, aqu1, aqu1_kw, aqu2, aqu2_kw, budg, budm, budm_kw, compnr, deleted, desc, llac, llac_kw, nbpr, sdbu, sdbu_kw, sequencenumber, text, text_ref_compnr, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ad10, 
                    src.ad10_kw, 
                    src.ad11, 
                    src.ad11_kw, 
                    src.ad12, 
                    src.ad12_kw, 
                    src.adt1, 
                    src.adt1_kw, 
                    src.adt2, 
                    src.adt2_kw, 
                    src.adt3, 
                    src.adt3_kw, 
                    src.adt4, 
                    src.adt4_kw, 
                    src.adt5, 
                    src.adt5_kw, 
                    src.adt6, 
                    src.adt6_kw, 
                    src.adt7, 
                    src.adt7_kw, 
                    src.adt8, 
                    src.adt8_kw, 
                    src.adt9, 
                    src.adt9_kw, 
                    src.aqu1, 
                    src.aqu1_kw, 
                    src.aqu2, 
                    src.aqu2_kw, 
                    src.budg, 
                    src.budm, 
                    src.budm_kw, 
                    src.compnr, 
                    src.deleted, 
                    src.desc, 
                    src.llac, 
                    src.llac_kw, 
                    src.nbpr, 
                    src.sdbu, 
                    src.sdbu_kw, 
                    src.sequencenumber, 
                    src.text, 
                    src.text_ref_compnr, 
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
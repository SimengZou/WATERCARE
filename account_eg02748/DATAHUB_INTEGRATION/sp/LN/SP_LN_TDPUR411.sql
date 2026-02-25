create or replace procedure DATAHUB_INTEGRATION.SP_LN_TDPUR411()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_LN_TDPUR411_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_LN.LN_TDPUR411 as target using (SELECT * FROM (SELECT 
            strm.atse, 
            strm.atse_ref_compnr, 
            strm.atsg, 
            strm.atsg_ref_compnr, 
            strm.citg, 
            strm.citg_ref_compnr, 
            strm.compnr, 
            strm.cpcl, 
            strm.cpcl_ref_compnr, 
            strm.cpgp, 
            strm.cpgp_ref_compnr, 
            strm.cpln, 
            strm.cpln_ref_compnr, 
            strm.csgp, 
            strm.csgp_ref_compnr, 
            strm.deleted, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.iwgt, 
            strm.iwun, 
            strm.iwun_ref_compnr, 
            strm.kitm, 
            strm.kitm_kw, 
            strm.orno, 
            strm.orno_pono_sqnb_ref_compnr, 
            strm.orno_ref_compnr, 
            strm.pgmd, 
            strm.pgmd_kw, 
            strm.pono, 
            strm.prtp, 
            strm.prtp_ref_compnr, 
            strm.sequencenumber, 
            strm.site, 
            strm.site_ref_compnr, 
            strm.sqnb, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.orno,
            strm.pono,
            strm.sqnb ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TDPUR411 as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.pono=src.pono) OR (target.pono IS NULL AND src.pono IS NULL)) AND
            ((target.sqnb=src.sqnb) OR (target.sqnb IS NULL AND src.sqnb IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.atse=src.atse, 
                    target.atse_ref_compnr=src.atse_ref_compnr, 
                    target.atsg=src.atsg, 
                    target.atsg_ref_compnr=src.atsg_ref_compnr, 
                    target.citg=src.citg, 
                    target.citg_ref_compnr=src.citg_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpcl=src.cpcl, 
                    target.cpcl_ref_compnr=src.cpcl_ref_compnr, 
                    target.cpgp=src.cpgp, 
                    target.cpgp_ref_compnr=src.cpgp_ref_compnr, 
                    target.cpln=src.cpln, 
                    target.cpln_ref_compnr=src.cpln_ref_compnr, 
                    target.csgp=src.csgp, 
                    target.csgp_ref_compnr=src.csgp_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.iwgt=src.iwgt, 
                    target.iwun=src.iwun, 
                    target.iwun_ref_compnr=src.iwun_ref_compnr, 
                    target.kitm=src.kitm, 
                    target.kitm_kw=src.kitm_kw, 
                    target.orno=src.orno, 
                    target.orno_pono_sqnb_ref_compnr=src.orno_pono_sqnb_ref_compnr, 
                    target.orno_ref_compnr=src.orno_ref_compnr, 
                    target.pgmd=src.pgmd, 
                    target.pgmd_kw=src.pgmd_kw, 
                    target.pono=src.pono, 
                    target.prtp=src.prtp, 
                    target.prtp_ref_compnr=src.prtp_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.site=src.site, 
                    target.site_ref_compnr=src.site_ref_compnr, 
                    target.sqnb=src.sqnb, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    atse, 
                    atse_ref_compnr, 
                    atsg, 
                    atsg_ref_compnr, 
                    citg, 
                    citg_ref_compnr, 
                    compnr, 
                    cpcl, 
                    cpcl_ref_compnr, 
                    cpgp, 
                    cpgp_ref_compnr, 
                    cpln, 
                    cpln_ref_compnr, 
                    csgp, 
                    csgp_ref_compnr, 
                    deleted, 
                    item, 
                    item_ref_compnr, 
                    iwgt, 
                    iwun, 
                    iwun_ref_compnr, 
                    kitm, 
                    kitm_kw, 
                    orno, 
                    orno_pono_sqnb_ref_compnr, 
                    orno_ref_compnr, 
                    pgmd, 
                    pgmd_kw, 
                    pono, 
                    prtp, 
                    prtp_ref_compnr, 
                    sequencenumber, 
                    site, 
                    site_ref_compnr, 
                    sqnb, 
                    timestamp, 
                    username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.atse, 
                    src.atse_ref_compnr, 
                    src.atsg, 
                    src.atsg_ref_compnr, 
                    src.citg, 
                    src.citg_ref_compnr, 
                    src.compnr, 
                    src.cpcl, 
                    src.cpcl_ref_compnr, 
                    src.cpgp, 
                    src.cpgp_ref_compnr, 
                    src.cpln, 
                    src.cpln_ref_compnr, 
                    src.csgp, 
                    src.csgp_ref_compnr, 
                    src.deleted, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.iwgt, 
                    src.iwun, 
                    src.iwun_ref_compnr, 
                    src.kitm, 
                    src.kitm_kw, 
                    src.orno, 
                    src.orno_pono_sqnb_ref_compnr, 
                    src.orno_ref_compnr, 
                    src.pgmd, 
                    src.pgmd_kw, 
                    src.pono, 
                    src.prtp, 
                    src.prtp_ref_compnr, 
                    src.sequencenumber, 
                    src.site, 
                    src.site_ref_compnr, 
                    src.sqnb, 
                    src.timestamp, 
                    src.username,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_LN.LN_TDPUR411_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.atse, 
            strm.atse_ref_compnr, 
            strm.atsg, 
            strm.atsg_ref_compnr, 
            strm.citg, 
            strm.citg_ref_compnr, 
            strm.compnr, 
            strm.cpcl, 
            strm.cpcl_ref_compnr, 
            strm.cpgp, 
            strm.cpgp_ref_compnr, 
            strm.cpln, 
            strm.cpln_ref_compnr, 
            strm.csgp, 
            strm.csgp_ref_compnr, 
            strm.deleted, 
            strm.item, 
            strm.item_ref_compnr, 
            strm.iwgt, 
            strm.iwun, 
            strm.iwun_ref_compnr, 
            strm.kitm, 
            strm.kitm_kw, 
            strm.orno, 
            strm.orno_pono_sqnb_ref_compnr, 
            strm.orno_ref_compnr, 
            strm.pgmd, 
            strm.pgmd_kw, 
            strm.pono, 
            strm.prtp, 
            strm.prtp_ref_compnr, 
            strm.sequencenumber, 
            strm.site, 
            strm.site_ref_compnr, 
            strm.sqnb, 
            strm.timestamp, 
            strm.username, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.compnr,
            strm.orno,
            strm.pono,
            strm.sqnb ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_LN_TDPUR411 as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.compnr=src.compnr) OR (target.compnr IS NULL AND src.compnr IS NULL)) AND
            ((target.orno=src.orno) OR (target.orno IS NULL AND src.orno IS NULL)) AND
            ((target.pono=src.pono) OR (target.pono IS NULL AND src.pono IS NULL)) AND
            ((target.sqnb=src.sqnb) OR (target.sqnb IS NULL AND src.sqnb IS NULL)) 
                when matched then update set
                    target.atse=src.atse, 
                    target.atse_ref_compnr=src.atse_ref_compnr, 
                    target.atsg=src.atsg, 
                    target.atsg_ref_compnr=src.atsg_ref_compnr, 
                    target.citg=src.citg, 
                    target.citg_ref_compnr=src.citg_ref_compnr, 
                    target.compnr=src.compnr, 
                    target.cpcl=src.cpcl, 
                    target.cpcl_ref_compnr=src.cpcl_ref_compnr, 
                    target.cpgp=src.cpgp, 
                    target.cpgp_ref_compnr=src.cpgp_ref_compnr, 
                    target.cpln=src.cpln, 
                    target.cpln_ref_compnr=src.cpln_ref_compnr, 
                    target.csgp=src.csgp, 
                    target.csgp_ref_compnr=src.csgp_ref_compnr, 
                    target.deleted=src.deleted, 
                    target.item=src.item, 
                    target.item_ref_compnr=src.item_ref_compnr, 
                    target.iwgt=src.iwgt, 
                    target.iwun=src.iwun, 
                    target.iwun_ref_compnr=src.iwun_ref_compnr, 
                    target.kitm=src.kitm, 
                    target.kitm_kw=src.kitm_kw, 
                    target.orno=src.orno, 
                    target.orno_pono_sqnb_ref_compnr=src.orno_pono_sqnb_ref_compnr, 
                    target.orno_ref_compnr=src.orno_ref_compnr, 
                    target.pgmd=src.pgmd, 
                    target.pgmd_kw=src.pgmd_kw, 
                    target.pono=src.pono, 
                    target.prtp=src.prtp, 
                    target.prtp_ref_compnr=src.prtp_ref_compnr, 
                    target.sequencenumber=src.sequencenumber, 
                    target.site=src.site, 
                    target.site_ref_compnr=src.site_ref_compnr, 
                    target.sqnb=src.sqnb, 
                    target.timestamp=src.timestamp, 
                    target.username=src.username, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( atse, atse_ref_compnr, atsg, atsg_ref_compnr, citg, citg_ref_compnr, compnr, cpcl, cpcl_ref_compnr, cpgp, cpgp_ref_compnr, cpln, cpln_ref_compnr, csgp, csgp_ref_compnr, deleted, item, item_ref_compnr, iwgt, iwun, iwun_ref_compnr, kitm, kitm_kw, orno, orno_pono_sqnb_ref_compnr, orno_ref_compnr, pgmd, pgmd_kw, pono, prtp, prtp_ref_compnr, sequencenumber, site, site_ref_compnr, sqnb, timestamp, username, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.atse, 
                    src.atse_ref_compnr, 
                    src.atsg, 
                    src.atsg_ref_compnr, 
                    src.citg, 
                    src.citg_ref_compnr, 
                    src.compnr, 
                    src.cpcl, 
                    src.cpcl_ref_compnr, 
                    src.cpgp, 
                    src.cpgp_ref_compnr, 
                    src.cpln, 
                    src.cpln_ref_compnr, 
                    src.csgp, 
                    src.csgp_ref_compnr, 
                    src.deleted, 
                    src.item, 
                    src.item_ref_compnr, 
                    src.iwgt, 
                    src.iwun, 
                    src.iwun_ref_compnr, 
                    src.kitm, 
                    src.kitm_kw, 
                    src.orno, 
                    src.orno_pono_sqnb_ref_compnr, 
                    src.orno_ref_compnr, 
                    src.pgmd, 
                    src.pgmd_kw, 
                    src.pono, 
                    src.prtp, 
                    src.prtp_ref_compnr, 
                    src.sequencenumber, 
                    src.site, 
                    src.site_ref_compnr, 
                    src.sqnb, 
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
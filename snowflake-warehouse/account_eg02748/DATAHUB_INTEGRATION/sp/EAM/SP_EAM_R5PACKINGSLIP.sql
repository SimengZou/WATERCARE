create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5PACKINGSLIP()
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
                            INSERT INTO LANDING_ERROR.EAM_RAW_ERROR
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5PACKINGSLIP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5PACKINGSLIP as target using (SELECT * FROM (SELECT 
            strm.PSL_BIN, 
            strm.PSL_COMMENT, 
            strm.PSL_DELQTY, 
            strm.PSL_DELUOM, 
            strm.PSL_DOCK, 
            strm.PSL_LASTSAVED, 
            strm.PSL_LINE, 
            strm.PSL_MANLOT, 
            strm.PSL_MANLOTEXP, 
            strm.PSL_MULTIPLY, 
            strm.PSL_ORDER, 
            strm.PSL_ORDER_ORG, 
            strm.PSL_ORDLINE, 
            strm.PSL_PART, 
            strm.PSL_PART_ORG, 
            strm.PSL_RECVQTY, 
            strm.PSL_SUPPLIERREF, 
            strm.PSL_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PSL_DOCK,
            strm.PSL_LINE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PACKINGSLIP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PSL_DOCK=src.PSL_DOCK) OR (target.PSL_DOCK IS NULL AND src.PSL_DOCK IS NULL)) AND
            ((target.PSL_LINE=src.PSL_LINE) OR (target.PSL_LINE IS NULL AND src.PSL_LINE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.PSL_BIN=src.PSL_BIN, 
                    target.PSL_COMMENT=src.PSL_COMMENT, 
                    target.PSL_DELQTY=src.PSL_DELQTY, 
                    target.PSL_DELUOM=src.PSL_DELUOM, 
                    target.PSL_DOCK=src.PSL_DOCK, 
                    target.PSL_LASTSAVED=src.PSL_LASTSAVED, 
                    target.PSL_LINE=src.PSL_LINE, 
                    target.PSL_MANLOT=src.PSL_MANLOT, 
                    target.PSL_MANLOTEXP=src.PSL_MANLOTEXP, 
                    target.PSL_MULTIPLY=src.PSL_MULTIPLY, 
                    target.PSL_ORDER=src.PSL_ORDER, 
                    target.PSL_ORDER_ORG=src.PSL_ORDER_ORG, 
                    target.PSL_ORDLINE=src.PSL_ORDLINE, 
                    target.PSL_PART=src.PSL_PART, 
                    target.PSL_PART_ORG=src.PSL_PART_ORG, 
                    target.PSL_RECVQTY=src.PSL_RECVQTY, 
                    target.PSL_SUPPLIERREF=src.PSL_SUPPLIERREF, 
                    target.PSL_UPDATECOUNT=src.PSL_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    PSL_BIN, 
                    PSL_COMMENT, 
                    PSL_DELQTY, 
                    PSL_DELUOM, 
                    PSL_DOCK, 
                    PSL_LASTSAVED, 
                    PSL_LINE, 
                    PSL_MANLOT, 
                    PSL_MANLOTEXP, 
                    PSL_MULTIPLY, 
                    PSL_ORDER, 
                    PSL_ORDER_ORG, 
                    PSL_ORDLINE, 
                    PSL_PART, 
                    PSL_PART_ORG, 
                    PSL_RECVQTY, 
                    PSL_SUPPLIERREF, 
                    PSL_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.PSL_BIN, 
                    src.PSL_COMMENT, 
                    src.PSL_DELQTY, 
                    src.PSL_DELUOM, 
                    src.PSL_DOCK, 
                    src.PSL_LASTSAVED, 
                    src.PSL_LINE, 
                    src.PSL_MANLOT, 
                    src.PSL_MANLOTEXP, 
                    src.PSL_MULTIPLY, 
                    src.PSL_ORDER, 
                    src.PSL_ORDER_ORG, 
                    src.PSL_ORDLINE, 
                    src.PSL_PART, 
                    src.PSL_PART_ORG, 
                    src.PSL_RECVQTY, 
                    src.PSL_SUPPLIERREF, 
                    src.PSL_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5PACKINGSLIP_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.PSL_BIN, 
            strm.PSL_COMMENT, 
            strm.PSL_DELQTY, 
            strm.PSL_DELUOM, 
            strm.PSL_DOCK, 
            strm.PSL_LASTSAVED, 
            strm.PSL_LINE, 
            strm.PSL_MANLOT, 
            strm.PSL_MANLOTEXP, 
            strm.PSL_MULTIPLY, 
            strm.PSL_ORDER, 
            strm.PSL_ORDER_ORG, 
            strm.PSL_ORDLINE, 
            strm.PSL_PART, 
            strm.PSL_PART_ORG, 
            strm.PSL_RECVQTY, 
            strm.PSL_SUPPLIERREF, 
            strm.PSL_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PSL_DOCK,
            strm.PSL_LINE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PACKINGSLIP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PSL_DOCK=src.PSL_DOCK) OR (target.PSL_DOCK IS NULL AND src.PSL_DOCK IS NULL)) AND
            ((target.PSL_LINE=src.PSL_LINE) OR (target.PSL_LINE IS NULL AND src.PSL_LINE IS NULL)) 
                when matched then update set
                    target.PSL_BIN=src.PSL_BIN, 
                    target.PSL_COMMENT=src.PSL_COMMENT, 
                    target.PSL_DELQTY=src.PSL_DELQTY, 
                    target.PSL_DELUOM=src.PSL_DELUOM, 
                    target.PSL_DOCK=src.PSL_DOCK, 
                    target.PSL_LASTSAVED=src.PSL_LASTSAVED, 
                    target.PSL_LINE=src.PSL_LINE, 
                    target.PSL_MANLOT=src.PSL_MANLOT, 
                    target.PSL_MANLOTEXP=src.PSL_MANLOTEXP, 
                    target.PSL_MULTIPLY=src.PSL_MULTIPLY, 
                    target.PSL_ORDER=src.PSL_ORDER, 
                    target.PSL_ORDER_ORG=src.PSL_ORDER_ORG, 
                    target.PSL_ORDLINE=src.PSL_ORDLINE, 
                    target.PSL_PART=src.PSL_PART, 
                    target.PSL_PART_ORG=src.PSL_PART_ORG, 
                    target.PSL_RECVQTY=src.PSL_RECVQTY, 
                    target.PSL_SUPPLIERREF=src.PSL_SUPPLIERREF, 
                    target.PSL_UPDATECOUNT=src.PSL_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( PSL_BIN, PSL_COMMENT, PSL_DELQTY, PSL_DELUOM, PSL_DOCK, PSL_LASTSAVED, PSL_LINE, PSL_MANLOT, PSL_MANLOTEXP, PSL_MULTIPLY, PSL_ORDER, PSL_ORDER_ORG, PSL_ORDLINE, PSL_PART, PSL_PART_ORG, PSL_RECVQTY, PSL_SUPPLIERREF, PSL_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.PSL_BIN, 
                    src.PSL_COMMENT, 
                    src.PSL_DELQTY, 
                    src.PSL_DELUOM, 
                    src.PSL_DOCK, 
                    src.PSL_LASTSAVED, 
                    src.PSL_LINE, 
                    src.PSL_MANLOT, 
                    src.PSL_MANLOTEXP, 
                    src.PSL_MULTIPLY, 
                    src.PSL_ORDER, 
                    src.PSL_ORDER_ORG, 
                    src.PSL_ORDLINE, 
                    src.PSL_PART, 
                    src.PSL_PART_ORG, 
                    src.PSL_RECVQTY, 
                    src.PSL_SUPPLIERREF, 
                    src.PSL_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
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
create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5STRUCTURES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5STRUCTURES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5STRUCTURES as target using (SELECT * FROM (SELECT 
            strm.STC_CHILD, 
            strm.STC_CHILDRTYPE, 
            strm.STC_CHILDTYPE, 
            strm.STC_CHILD_ORG, 
            strm.STC_DOWNTIME, 
            strm.STC_EQUIVALENT, 
            strm.STC_LASTSAVED, 
            strm.STC_MNBDEFINITION, 
            strm.STC_PARENT, 
            strm.STC_PARENTRTYPE, 
            strm.STC_PARENTTYPE, 
            strm.STC_PARENT_ORG, 
            strm.STC_ROLLDOWN, 
            strm.STC_ROLLUP, 
            strm.STC_SEQUENCE, 
            strm.STC_UPDATECOUNT, 
            strm.STC_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.STC_CHILD,
            strm.STC_CHILD_ORG,
            strm.STC_PARENT,
            strm.STC_PARENT_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5STRUCTURES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.STC_CHILD=src.STC_CHILD) OR (target.STC_CHILD IS NULL AND src.STC_CHILD IS NULL)) AND
            ((target.STC_CHILD_ORG=src.STC_CHILD_ORG) OR (target.STC_CHILD_ORG IS NULL AND src.STC_CHILD_ORG IS NULL)) AND
            ((target.STC_PARENT=src.STC_PARENT) OR (target.STC_PARENT IS NULL AND src.STC_PARENT IS NULL)) AND
            ((target.STC_PARENT_ORG=src.STC_PARENT_ORG) OR (target.STC_PARENT_ORG IS NULL AND src.STC_PARENT_ORG IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.STC_CHILD=src.STC_CHILD, 
                    target.STC_CHILDRTYPE=src.STC_CHILDRTYPE, 
                    target.STC_CHILDTYPE=src.STC_CHILDTYPE, 
                    target.STC_CHILD_ORG=src.STC_CHILD_ORG, 
                    target.STC_DOWNTIME=src.STC_DOWNTIME, 
                    target.STC_EQUIVALENT=src.STC_EQUIVALENT, 
                    target.STC_LASTSAVED=src.STC_LASTSAVED, 
                    target.STC_MNBDEFINITION=src.STC_MNBDEFINITION, 
                    target.STC_PARENT=src.STC_PARENT, 
                    target.STC_PARENTRTYPE=src.STC_PARENTRTYPE, 
                    target.STC_PARENTTYPE=src.STC_PARENTTYPE, 
                    target.STC_PARENT_ORG=src.STC_PARENT_ORG, 
                    target.STC_ROLLDOWN=src.STC_ROLLDOWN, 
                    target.STC_ROLLUP=src.STC_ROLLUP, 
                    target.STC_SEQUENCE=src.STC_SEQUENCE, 
                    target.STC_UPDATECOUNT=src.STC_UPDATECOUNT, 
                    target.STC_UPDATED=src.STC_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    STC_CHILD, 
                    STC_CHILDRTYPE, 
                    STC_CHILDTYPE, 
                    STC_CHILD_ORG, 
                    STC_DOWNTIME, 
                    STC_EQUIVALENT, 
                    STC_LASTSAVED, 
                    STC_MNBDEFINITION, 
                    STC_PARENT, 
                    STC_PARENTRTYPE, 
                    STC_PARENTTYPE, 
                    STC_PARENT_ORG, 
                    STC_ROLLDOWN, 
                    STC_ROLLUP, 
                    STC_SEQUENCE, 
                    STC_UPDATECOUNT, 
                    STC_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.STC_CHILD, 
                    src.STC_CHILDRTYPE, 
                    src.STC_CHILDTYPE, 
                    src.STC_CHILD_ORG, 
                    src.STC_DOWNTIME, 
                    src.STC_EQUIVALENT, 
                    src.STC_LASTSAVED, 
                    src.STC_MNBDEFINITION, 
                    src.STC_PARENT, 
                    src.STC_PARENTRTYPE, 
                    src.STC_PARENTTYPE, 
                    src.STC_PARENT_ORG, 
                    src.STC_ROLLDOWN, 
                    src.STC_ROLLUP, 
                    src.STC_SEQUENCE, 
                    src.STC_UPDATECOUNT, 
                    src.STC_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5STRUCTURES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.STC_CHILD, 
            strm.STC_CHILDRTYPE, 
            strm.STC_CHILDTYPE, 
            strm.STC_CHILD_ORG, 
            strm.STC_DOWNTIME, 
            strm.STC_EQUIVALENT, 
            strm.STC_LASTSAVED, 
            strm.STC_MNBDEFINITION, 
            strm.STC_PARENT, 
            strm.STC_PARENTRTYPE, 
            strm.STC_PARENTTYPE, 
            strm.STC_PARENT_ORG, 
            strm.STC_ROLLDOWN, 
            strm.STC_ROLLUP, 
            strm.STC_SEQUENCE, 
            strm.STC_UPDATECOUNT, 
            strm.STC_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.STC_CHILD,
            strm.STC_CHILD_ORG,
            strm.STC_PARENT,
            strm.STC_PARENT_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5STRUCTURES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.STC_CHILD=src.STC_CHILD) OR (target.STC_CHILD IS NULL AND src.STC_CHILD IS NULL)) AND
            ((target.STC_CHILD_ORG=src.STC_CHILD_ORG) OR (target.STC_CHILD_ORG IS NULL AND src.STC_CHILD_ORG IS NULL)) AND
            ((target.STC_PARENT=src.STC_PARENT) OR (target.STC_PARENT IS NULL AND src.STC_PARENT IS NULL)) AND
            ((target.STC_PARENT_ORG=src.STC_PARENT_ORG) OR (target.STC_PARENT_ORG IS NULL AND src.STC_PARENT_ORG IS NULL)) 
                when matched then update set
                    target.STC_CHILD=src.STC_CHILD, 
                    target.STC_CHILDRTYPE=src.STC_CHILDRTYPE, 
                    target.STC_CHILDTYPE=src.STC_CHILDTYPE, 
                    target.STC_CHILD_ORG=src.STC_CHILD_ORG, 
                    target.STC_DOWNTIME=src.STC_DOWNTIME, 
                    target.STC_EQUIVALENT=src.STC_EQUIVALENT, 
                    target.STC_LASTSAVED=src.STC_LASTSAVED, 
                    target.STC_MNBDEFINITION=src.STC_MNBDEFINITION, 
                    target.STC_PARENT=src.STC_PARENT, 
                    target.STC_PARENTRTYPE=src.STC_PARENTRTYPE, 
                    target.STC_PARENTTYPE=src.STC_PARENTTYPE, 
                    target.STC_PARENT_ORG=src.STC_PARENT_ORG, 
                    target.STC_ROLLDOWN=src.STC_ROLLDOWN, 
                    target.STC_ROLLUP=src.STC_ROLLUP, 
                    target.STC_SEQUENCE=src.STC_SEQUENCE, 
                    target.STC_UPDATECOUNT=src.STC_UPDATECOUNT, 
                    target.STC_UPDATED=src.STC_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( STC_CHILD, STC_CHILDRTYPE, STC_CHILDTYPE, STC_CHILD_ORG, STC_DOWNTIME, STC_EQUIVALENT, STC_LASTSAVED, STC_MNBDEFINITION, STC_PARENT, STC_PARENTRTYPE, STC_PARENTTYPE, STC_PARENT_ORG, STC_ROLLDOWN, STC_ROLLUP, STC_SEQUENCE, STC_UPDATECOUNT, STC_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.STC_CHILD, 
                    src.STC_CHILDRTYPE, 
                    src.STC_CHILDTYPE, 
                    src.STC_CHILD_ORG, 
                    src.STC_DOWNTIME, 
                    src.STC_EQUIVALENT, 
                    src.STC_LASTSAVED, 
                    src.STC_MNBDEFINITION, 
                    src.STC_PARENT, 
                    src.STC_PARENTRTYPE, 
                    src.STC_PARENTTYPE, 
                    src.STC_PARENT_ORG, 
                    src.STC_ROLLDOWN, 
                    src.STC_ROLLUP, 
                    src.STC_SEQUENCE, 
                    src.STC_UPDATECOUNT, 
                    src.STC_UPDATED, 
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
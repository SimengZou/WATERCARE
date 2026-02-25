create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ENTITIES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ENTITIES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ENTITIES as target using (SELECT * FROM (SELECT 
            strm.ENT_ADDATTRIBS, 
            strm.ENT_ADDRESSES, 
            strm.ENT_ASSPARTS, 
            strm.ENT_ASSPERMITS, 
            strm.ENT_AUDITS, 
            strm.ENT_CAAUDIT, 
            strm.ENT_CLASSIF, 
            strm.ENT_DOCUMENTS, 
            strm.ENT_ENTITY, 
            strm.ENT_ERECORD, 
            strm.ENT_FREETEXT, 
            strm.ENT_FTAUDIT, 
            strm.ENT_LASTSAVED, 
            strm.ENT_MULTILANG, 
            strm.ENT_RENTITY, 
            strm.ENT_STATENT, 
            strm.ENT_TABLE, 
            strm.ENT_TYPENT, 
            strm.ENT_UPDATECOUNT, 
            strm.ENT_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ENT_RENTITY ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ENTITIES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ENT_RENTITY=src.ENT_RENTITY) OR (target.ENT_RENTITY IS NULL AND src.ENT_RENTITY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ENT_ADDATTRIBS=src.ENT_ADDATTRIBS, 
                    target.ENT_ADDRESSES=src.ENT_ADDRESSES, 
                    target.ENT_ASSPARTS=src.ENT_ASSPARTS, 
                    target.ENT_ASSPERMITS=src.ENT_ASSPERMITS, 
                    target.ENT_AUDITS=src.ENT_AUDITS, 
                    target.ENT_CAAUDIT=src.ENT_CAAUDIT, 
                    target.ENT_CLASSIF=src.ENT_CLASSIF, 
                    target.ENT_DOCUMENTS=src.ENT_DOCUMENTS, 
                    target.ENT_ENTITY=src.ENT_ENTITY, 
                    target.ENT_ERECORD=src.ENT_ERECORD, 
                    target.ENT_FREETEXT=src.ENT_FREETEXT, 
                    target.ENT_FTAUDIT=src.ENT_FTAUDIT, 
                    target.ENT_LASTSAVED=src.ENT_LASTSAVED, 
                    target.ENT_MULTILANG=src.ENT_MULTILANG, 
                    target.ENT_RENTITY=src.ENT_RENTITY, 
                    target.ENT_STATENT=src.ENT_STATENT, 
                    target.ENT_TABLE=src.ENT_TABLE, 
                    target.ENT_TYPENT=src.ENT_TYPENT, 
                    target.ENT_UPDATECOUNT=src.ENT_UPDATECOUNT, 
                    target.ENT_UPDATED=src.ENT_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ENT_ADDATTRIBS, 
                    ENT_ADDRESSES, 
                    ENT_ASSPARTS, 
                    ENT_ASSPERMITS, 
                    ENT_AUDITS, 
                    ENT_CAAUDIT, 
                    ENT_CLASSIF, 
                    ENT_DOCUMENTS, 
                    ENT_ENTITY, 
                    ENT_ERECORD, 
                    ENT_FREETEXT, 
                    ENT_FTAUDIT, 
                    ENT_LASTSAVED, 
                    ENT_MULTILANG, 
                    ENT_RENTITY, 
                    ENT_STATENT, 
                    ENT_TABLE, 
                    ENT_TYPENT, 
                    ENT_UPDATECOUNT, 
                    ENT_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ENT_ADDATTRIBS, 
                    src.ENT_ADDRESSES, 
                    src.ENT_ASSPARTS, 
                    src.ENT_ASSPERMITS, 
                    src.ENT_AUDITS, 
                    src.ENT_CAAUDIT, 
                    src.ENT_CLASSIF, 
                    src.ENT_DOCUMENTS, 
                    src.ENT_ENTITY, 
                    src.ENT_ERECORD, 
                    src.ENT_FREETEXT, 
                    src.ENT_FTAUDIT, 
                    src.ENT_LASTSAVED, 
                    src.ENT_MULTILANG, 
                    src.ENT_RENTITY, 
                    src.ENT_STATENT, 
                    src.ENT_TABLE, 
                    src.ENT_TYPENT, 
                    src.ENT_UPDATECOUNT, 
                    src.ENT_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ENTITIES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ENT_ADDATTRIBS, 
            strm.ENT_ADDRESSES, 
            strm.ENT_ASSPARTS, 
            strm.ENT_ASSPERMITS, 
            strm.ENT_AUDITS, 
            strm.ENT_CAAUDIT, 
            strm.ENT_CLASSIF, 
            strm.ENT_DOCUMENTS, 
            strm.ENT_ENTITY, 
            strm.ENT_ERECORD, 
            strm.ENT_FREETEXT, 
            strm.ENT_FTAUDIT, 
            strm.ENT_LASTSAVED, 
            strm.ENT_MULTILANG, 
            strm.ENT_RENTITY, 
            strm.ENT_STATENT, 
            strm.ENT_TABLE, 
            strm.ENT_TYPENT, 
            strm.ENT_UPDATECOUNT, 
            strm.ENT_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ENT_RENTITY ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ENTITIES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ENT_RENTITY=src.ENT_RENTITY) OR (target.ENT_RENTITY IS NULL AND src.ENT_RENTITY IS NULL)) 
                when matched then update set
                    target.ENT_ADDATTRIBS=src.ENT_ADDATTRIBS, 
                    target.ENT_ADDRESSES=src.ENT_ADDRESSES, 
                    target.ENT_ASSPARTS=src.ENT_ASSPARTS, 
                    target.ENT_ASSPERMITS=src.ENT_ASSPERMITS, 
                    target.ENT_AUDITS=src.ENT_AUDITS, 
                    target.ENT_CAAUDIT=src.ENT_CAAUDIT, 
                    target.ENT_CLASSIF=src.ENT_CLASSIF, 
                    target.ENT_DOCUMENTS=src.ENT_DOCUMENTS, 
                    target.ENT_ENTITY=src.ENT_ENTITY, 
                    target.ENT_ERECORD=src.ENT_ERECORD, 
                    target.ENT_FREETEXT=src.ENT_FREETEXT, 
                    target.ENT_FTAUDIT=src.ENT_FTAUDIT, 
                    target.ENT_LASTSAVED=src.ENT_LASTSAVED, 
                    target.ENT_MULTILANG=src.ENT_MULTILANG, 
                    target.ENT_RENTITY=src.ENT_RENTITY, 
                    target.ENT_STATENT=src.ENT_STATENT, 
                    target.ENT_TABLE=src.ENT_TABLE, 
                    target.ENT_TYPENT=src.ENT_TYPENT, 
                    target.ENT_UPDATECOUNT=src.ENT_UPDATECOUNT, 
                    target.ENT_UPDATED=src.ENT_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ENT_ADDATTRIBS, ENT_ADDRESSES, ENT_ASSPARTS, ENT_ASSPERMITS, ENT_AUDITS, ENT_CAAUDIT, ENT_CLASSIF, ENT_DOCUMENTS, ENT_ENTITY, ENT_ERECORD, ENT_FREETEXT, ENT_FTAUDIT, ENT_LASTSAVED, ENT_MULTILANG, ENT_RENTITY, ENT_STATENT, ENT_TABLE, ENT_TYPENT, ENT_UPDATECOUNT, ENT_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ENT_ADDATTRIBS, 
                    src.ENT_ADDRESSES, 
                    src.ENT_ASSPARTS, 
                    src.ENT_ASSPERMITS, 
                    src.ENT_AUDITS, 
                    src.ENT_CAAUDIT, 
                    src.ENT_CLASSIF, 
                    src.ENT_DOCUMENTS, 
                    src.ENT_ENTITY, 
                    src.ENT_ERECORD, 
                    src.ENT_FREETEXT, 
                    src.ENT_FTAUDIT, 
                    src.ENT_LASTSAVED, 
                    src.ENT_MULTILANG, 
                    src.ENT_RENTITY, 
                    src.ENT_STATENT, 
                    src.ENT_TABLE, 
                    src.ENT_TYPENT, 
                    src.ENT_UPDATECOUNT, 
                    src.ENT_UPDATED, 
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
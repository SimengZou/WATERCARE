create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5REPPARMS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5REPPARMS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5REPPARMS as target using (SELECT * FROM (SELECT 
            strm.PMT_BOTTEXT, 
            strm.PMT_DATATYPE, 
            strm.PMT_DEFAULT, 
            strm.PMT_EWSLOVDEF, 
            strm.PMT_FIXED, 
            strm.PMT_FORMAT, 
            strm.PMT_FUNCTION, 
            strm.PMT_LASTSAVED, 
            strm.PMT_LENGTH, 
            strm.PMT_LINE, 
            strm.PMT_LOVFUNCTION, 
            strm.PMT_MANDATORY, 
            strm.PMT_MEKEY, 
            strm.PMT_OPTIONS, 
            strm.PMT_PARAMETER, 
            strm.PMT_PROPERTY, 
            strm.PMT_QUERY, 
            strm.PMT_REMEMBER, 
            strm.PMT_RENTITY, 
            strm.PMT_RTYPE, 
            strm.PMT_TEST, 
            strm.PMT_UPDATECOUNT, 
            strm.PMT_UPDATED, 
            strm.PMT_UPPER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PMT_FUNCTION,
            strm.PMT_PARAMETER ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPPARMS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PMT_FUNCTION=src.PMT_FUNCTION) OR (target.PMT_FUNCTION IS NULL AND src.PMT_FUNCTION IS NULL)) AND
            ((target.PMT_PARAMETER=src.PMT_PARAMETER) OR (target.PMT_PARAMETER IS NULL AND src.PMT_PARAMETER IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.PMT_BOTTEXT=src.PMT_BOTTEXT, 
                    target.PMT_DATATYPE=src.PMT_DATATYPE, 
                    target.PMT_DEFAULT=src.PMT_DEFAULT, 
                    target.PMT_EWSLOVDEF=src.PMT_EWSLOVDEF, 
                    target.PMT_FIXED=src.PMT_FIXED, 
                    target.PMT_FORMAT=src.PMT_FORMAT, 
                    target.PMT_FUNCTION=src.PMT_FUNCTION, 
                    target.PMT_LASTSAVED=src.PMT_LASTSAVED, 
                    target.PMT_LENGTH=src.PMT_LENGTH, 
                    target.PMT_LINE=src.PMT_LINE, 
                    target.PMT_LOVFUNCTION=src.PMT_LOVFUNCTION, 
                    target.PMT_MANDATORY=src.PMT_MANDATORY, 
                    target.PMT_MEKEY=src.PMT_MEKEY, 
                    target.PMT_OPTIONS=src.PMT_OPTIONS, 
                    target.PMT_PARAMETER=src.PMT_PARAMETER, 
                    target.PMT_PROPERTY=src.PMT_PROPERTY, 
                    target.PMT_QUERY=src.PMT_QUERY, 
                    target.PMT_REMEMBER=src.PMT_REMEMBER, 
                    target.PMT_RENTITY=src.PMT_RENTITY, 
                    target.PMT_RTYPE=src.PMT_RTYPE, 
                    target.PMT_TEST=src.PMT_TEST, 
                    target.PMT_UPDATECOUNT=src.PMT_UPDATECOUNT, 
                    target.PMT_UPDATED=src.PMT_UPDATED, 
                    target.PMT_UPPER=src.PMT_UPPER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    PMT_BOTTEXT, 
                    PMT_DATATYPE, 
                    PMT_DEFAULT, 
                    PMT_EWSLOVDEF, 
                    PMT_FIXED, 
                    PMT_FORMAT, 
                    PMT_FUNCTION, 
                    PMT_LASTSAVED, 
                    PMT_LENGTH, 
                    PMT_LINE, 
                    PMT_LOVFUNCTION, 
                    PMT_MANDATORY, 
                    PMT_MEKEY, 
                    PMT_OPTIONS, 
                    PMT_PARAMETER, 
                    PMT_PROPERTY, 
                    PMT_QUERY, 
                    PMT_REMEMBER, 
                    PMT_RENTITY, 
                    PMT_RTYPE, 
                    PMT_TEST, 
                    PMT_UPDATECOUNT, 
                    PMT_UPDATED, 
                    PMT_UPPER, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.PMT_BOTTEXT, 
                    src.PMT_DATATYPE, 
                    src.PMT_DEFAULT, 
                    src.PMT_EWSLOVDEF, 
                    src.PMT_FIXED, 
                    src.PMT_FORMAT, 
                    src.PMT_FUNCTION, 
                    src.PMT_LASTSAVED, 
                    src.PMT_LENGTH, 
                    src.PMT_LINE, 
                    src.PMT_LOVFUNCTION, 
                    src.PMT_MANDATORY, 
                    src.PMT_MEKEY, 
                    src.PMT_OPTIONS, 
                    src.PMT_PARAMETER, 
                    src.PMT_PROPERTY, 
                    src.PMT_QUERY, 
                    src.PMT_REMEMBER, 
                    src.PMT_RENTITY, 
                    src.PMT_RTYPE, 
                    src.PMT_TEST, 
                    src.PMT_UPDATECOUNT, 
                    src.PMT_UPDATED, 
                    src.PMT_UPPER, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5REPPARMS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.PMT_BOTTEXT, 
            strm.PMT_DATATYPE, 
            strm.PMT_DEFAULT, 
            strm.PMT_EWSLOVDEF, 
            strm.PMT_FIXED, 
            strm.PMT_FORMAT, 
            strm.PMT_FUNCTION, 
            strm.PMT_LASTSAVED, 
            strm.PMT_LENGTH, 
            strm.PMT_LINE, 
            strm.PMT_LOVFUNCTION, 
            strm.PMT_MANDATORY, 
            strm.PMT_MEKEY, 
            strm.PMT_OPTIONS, 
            strm.PMT_PARAMETER, 
            strm.PMT_PROPERTY, 
            strm.PMT_QUERY, 
            strm.PMT_REMEMBER, 
            strm.PMT_RENTITY, 
            strm.PMT_RTYPE, 
            strm.PMT_TEST, 
            strm.PMT_UPDATECOUNT, 
            strm.PMT_UPDATED, 
            strm.PMT_UPPER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PMT_FUNCTION,
            strm.PMT_PARAMETER ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPPARMS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PMT_FUNCTION=src.PMT_FUNCTION) OR (target.PMT_FUNCTION IS NULL AND src.PMT_FUNCTION IS NULL)) AND
            ((target.PMT_PARAMETER=src.PMT_PARAMETER) OR (target.PMT_PARAMETER IS NULL AND src.PMT_PARAMETER IS NULL)) 
                when matched then update set
                    target.PMT_BOTTEXT=src.PMT_BOTTEXT, 
                    target.PMT_DATATYPE=src.PMT_DATATYPE, 
                    target.PMT_DEFAULT=src.PMT_DEFAULT, 
                    target.PMT_EWSLOVDEF=src.PMT_EWSLOVDEF, 
                    target.PMT_FIXED=src.PMT_FIXED, 
                    target.PMT_FORMAT=src.PMT_FORMAT, 
                    target.PMT_FUNCTION=src.PMT_FUNCTION, 
                    target.PMT_LASTSAVED=src.PMT_LASTSAVED, 
                    target.PMT_LENGTH=src.PMT_LENGTH, 
                    target.PMT_LINE=src.PMT_LINE, 
                    target.PMT_LOVFUNCTION=src.PMT_LOVFUNCTION, 
                    target.PMT_MANDATORY=src.PMT_MANDATORY, 
                    target.PMT_MEKEY=src.PMT_MEKEY, 
                    target.PMT_OPTIONS=src.PMT_OPTIONS, 
                    target.PMT_PARAMETER=src.PMT_PARAMETER, 
                    target.PMT_PROPERTY=src.PMT_PROPERTY, 
                    target.PMT_QUERY=src.PMT_QUERY, 
                    target.PMT_REMEMBER=src.PMT_REMEMBER, 
                    target.PMT_RENTITY=src.PMT_RENTITY, 
                    target.PMT_RTYPE=src.PMT_RTYPE, 
                    target.PMT_TEST=src.PMT_TEST, 
                    target.PMT_UPDATECOUNT=src.PMT_UPDATECOUNT, 
                    target.PMT_UPDATED=src.PMT_UPDATED, 
                    target.PMT_UPPER=src.PMT_UPPER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( PMT_BOTTEXT, PMT_DATATYPE, PMT_DEFAULT, PMT_EWSLOVDEF, PMT_FIXED, PMT_FORMAT, PMT_FUNCTION, PMT_LASTSAVED, PMT_LENGTH, PMT_LINE, PMT_LOVFUNCTION, PMT_MANDATORY, PMT_MEKEY, PMT_OPTIONS, PMT_PARAMETER, PMT_PROPERTY, PMT_QUERY, PMT_REMEMBER, PMT_RENTITY, PMT_RTYPE, PMT_TEST, PMT_UPDATECOUNT, PMT_UPDATED, PMT_UPPER, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.PMT_BOTTEXT, 
                    src.PMT_DATATYPE, 
                    src.PMT_DEFAULT, 
                    src.PMT_EWSLOVDEF, 
                    src.PMT_FIXED, 
                    src.PMT_FORMAT, 
                    src.PMT_FUNCTION, 
                    src.PMT_LASTSAVED, 
                    src.PMT_LENGTH, 
                    src.PMT_LINE, 
                    src.PMT_LOVFUNCTION, 
                    src.PMT_MANDATORY, 
                    src.PMT_MEKEY, 
                    src.PMT_OPTIONS, 
                    src.PMT_PARAMETER, 
                    src.PMT_PROPERTY, 
                    src.PMT_QUERY, 
                    src.PMT_REMEMBER, 
                    src.PMT_RENTITY, 
                    src.PMT_RTYPE, 
                    src.PMT_TEST, 
                    src.PMT_UPDATECOUNT, 
                    src.PMT_UPDATED, 
                    src.PMT_UPPER, 
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
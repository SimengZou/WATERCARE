create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5GRIDFIELD()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5GRIDFIELD_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5GRIDFIELD as target using (SELECT * FROM (SELECT 
            strm.GFD_AGGFUNC, 
            strm.GFD_BOTFUNCTION, 
            strm.GFD_BOTNUMBER, 
            strm.GFD_CONTROLTYPE, 
            strm.GFD_DEPENDENT, 
            strm.GFD_FIELDID, 
            strm.GFD_FIELDTYPE, 
            strm.GFD_GRIDID, 
            strm.GFD_HEADERLOCATION, 
            strm.GFD_LASTSAVED, 
            strm.GFD_OCCURRENCE, 
            strm.GFD_SCRIPTEVENT, 
            strm.GFD_SECENTITY, 
            strm.GFD_TAGNAME, 
            strm.GFD_TAGPARAMS, 
            strm.GFD_UPDATECOUNT, 
            strm.GFD_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.GFD_FIELDID,
            strm.GFD_GRIDID,
            strm.GFD_OCCURRENCE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5GRIDFIELD as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.GFD_FIELDID=src.GFD_FIELDID) OR (target.GFD_FIELDID IS NULL AND src.GFD_FIELDID IS NULL)) AND
            ((target.GFD_GRIDID=src.GFD_GRIDID) OR (target.GFD_GRIDID IS NULL AND src.GFD_GRIDID IS NULL)) AND
            ((target.GFD_OCCURRENCE=src.GFD_OCCURRENCE) OR (target.GFD_OCCURRENCE IS NULL AND src.GFD_OCCURRENCE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.GFD_AGGFUNC=src.GFD_AGGFUNC, 
                    target.GFD_BOTFUNCTION=src.GFD_BOTFUNCTION, 
                    target.GFD_BOTNUMBER=src.GFD_BOTNUMBER, 
                    target.GFD_CONTROLTYPE=src.GFD_CONTROLTYPE, 
                    target.GFD_DEPENDENT=src.GFD_DEPENDENT, 
                    target.GFD_FIELDID=src.GFD_FIELDID, 
                    target.GFD_FIELDTYPE=src.GFD_FIELDTYPE, 
                    target.GFD_GRIDID=src.GFD_GRIDID, 
                    target.GFD_HEADERLOCATION=src.GFD_HEADERLOCATION, 
                    target.GFD_LASTSAVED=src.GFD_LASTSAVED, 
                    target.GFD_OCCURRENCE=src.GFD_OCCURRENCE, 
                    target.GFD_SCRIPTEVENT=src.GFD_SCRIPTEVENT, 
                    target.GFD_SECENTITY=src.GFD_SECENTITY, 
                    target.GFD_TAGNAME=src.GFD_TAGNAME, 
                    target.GFD_TAGPARAMS=src.GFD_TAGPARAMS, 
                    target.GFD_UPDATECOUNT=src.GFD_UPDATECOUNT, 
                    target.GFD_UPDATED=src.GFD_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    GFD_AGGFUNC, 
                    GFD_BOTFUNCTION, 
                    GFD_BOTNUMBER, 
                    GFD_CONTROLTYPE, 
                    GFD_DEPENDENT, 
                    GFD_FIELDID, 
                    GFD_FIELDTYPE, 
                    GFD_GRIDID, 
                    GFD_HEADERLOCATION, 
                    GFD_LASTSAVED, 
                    GFD_OCCURRENCE, 
                    GFD_SCRIPTEVENT, 
                    GFD_SECENTITY, 
                    GFD_TAGNAME, 
                    GFD_TAGPARAMS, 
                    GFD_UPDATECOUNT, 
                    GFD_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.GFD_AGGFUNC, 
                    src.GFD_BOTFUNCTION, 
                    src.GFD_BOTNUMBER, 
                    src.GFD_CONTROLTYPE, 
                    src.GFD_DEPENDENT, 
                    src.GFD_FIELDID, 
                    src.GFD_FIELDTYPE, 
                    src.GFD_GRIDID, 
                    src.GFD_HEADERLOCATION, 
                    src.GFD_LASTSAVED, 
                    src.GFD_OCCURRENCE, 
                    src.GFD_SCRIPTEVENT, 
                    src.GFD_SECENTITY, 
                    src.GFD_TAGNAME, 
                    src.GFD_TAGPARAMS, 
                    src.GFD_UPDATECOUNT, 
                    src.GFD_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5GRIDFIELD_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.GFD_AGGFUNC, 
            strm.GFD_BOTFUNCTION, 
            strm.GFD_BOTNUMBER, 
            strm.GFD_CONTROLTYPE, 
            strm.GFD_DEPENDENT, 
            strm.GFD_FIELDID, 
            strm.GFD_FIELDTYPE, 
            strm.GFD_GRIDID, 
            strm.GFD_HEADERLOCATION, 
            strm.GFD_LASTSAVED, 
            strm.GFD_OCCURRENCE, 
            strm.GFD_SCRIPTEVENT, 
            strm.GFD_SECENTITY, 
            strm.GFD_TAGNAME, 
            strm.GFD_TAGPARAMS, 
            strm.GFD_UPDATECOUNT, 
            strm.GFD_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.GFD_FIELDID,
            strm.GFD_GRIDID,
            strm.GFD_OCCURRENCE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5GRIDFIELD as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.GFD_FIELDID=src.GFD_FIELDID) OR (target.GFD_FIELDID IS NULL AND src.GFD_FIELDID IS NULL)) AND
            ((target.GFD_GRIDID=src.GFD_GRIDID) OR (target.GFD_GRIDID IS NULL AND src.GFD_GRIDID IS NULL)) AND
            ((target.GFD_OCCURRENCE=src.GFD_OCCURRENCE) OR (target.GFD_OCCURRENCE IS NULL AND src.GFD_OCCURRENCE IS NULL)) 
                when matched then update set
                    target.GFD_AGGFUNC=src.GFD_AGGFUNC, 
                    target.GFD_BOTFUNCTION=src.GFD_BOTFUNCTION, 
                    target.GFD_BOTNUMBER=src.GFD_BOTNUMBER, 
                    target.GFD_CONTROLTYPE=src.GFD_CONTROLTYPE, 
                    target.GFD_DEPENDENT=src.GFD_DEPENDENT, 
                    target.GFD_FIELDID=src.GFD_FIELDID, 
                    target.GFD_FIELDTYPE=src.GFD_FIELDTYPE, 
                    target.GFD_GRIDID=src.GFD_GRIDID, 
                    target.GFD_HEADERLOCATION=src.GFD_HEADERLOCATION, 
                    target.GFD_LASTSAVED=src.GFD_LASTSAVED, 
                    target.GFD_OCCURRENCE=src.GFD_OCCURRENCE, 
                    target.GFD_SCRIPTEVENT=src.GFD_SCRIPTEVENT, 
                    target.GFD_SECENTITY=src.GFD_SECENTITY, 
                    target.GFD_TAGNAME=src.GFD_TAGNAME, 
                    target.GFD_TAGPARAMS=src.GFD_TAGPARAMS, 
                    target.GFD_UPDATECOUNT=src.GFD_UPDATECOUNT, 
                    target.GFD_UPDATED=src.GFD_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( GFD_AGGFUNC, GFD_BOTFUNCTION, GFD_BOTNUMBER, GFD_CONTROLTYPE, GFD_DEPENDENT, GFD_FIELDID, GFD_FIELDTYPE, GFD_GRIDID, GFD_HEADERLOCATION, GFD_LASTSAVED, GFD_OCCURRENCE, GFD_SCRIPTEVENT, GFD_SECENTITY, GFD_TAGNAME, GFD_TAGPARAMS, GFD_UPDATECOUNT, GFD_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.GFD_AGGFUNC, 
                    src.GFD_BOTFUNCTION, 
                    src.GFD_BOTNUMBER, 
                    src.GFD_CONTROLTYPE, 
                    src.GFD_DEPENDENT, 
                    src.GFD_FIELDID, 
                    src.GFD_FIELDTYPE, 
                    src.GFD_GRIDID, 
                    src.GFD_HEADERLOCATION, 
                    src.GFD_LASTSAVED, 
                    src.GFD_OCCURRENCE, 
                    src.GFD_SCRIPTEVENT, 
                    src.GFD_SECENTITY, 
                    src.GFD_TAGNAME, 
                    src.GFD_TAGPARAMS, 
                    src.GFD_UPDATECOUNT, 
                    src.GFD_UPDATED, 
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
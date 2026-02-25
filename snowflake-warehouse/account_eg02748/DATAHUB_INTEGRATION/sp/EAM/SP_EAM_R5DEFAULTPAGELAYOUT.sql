create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5DEFAULTPAGELAYOUT()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5DEFAULTPAGELAYOUT_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5DEFAULTPAGELAYOUT as target using (SELECT * FROM (SELECT 
            strm.PLD_ATTRIBUTE, 
            strm.PLD_CASE, 
            strm.PLD_DDFIELDNAME, 
            strm.PLD_DDTABLENAME, 
            strm.PLD_DESTXPATH, 
            strm.PLD_ELEMENTID, 
            strm.PLD_ELEMENTTYPE, 
            strm.PLD_FIELDCONTAINER, 
            strm.PLD_FIELDCONTTYPE, 
            strm.PLD_FIELDGROUP, 
            strm.PLD_FIELDTYPE, 
            strm.PLD_JSPFILE, 
            strm.PLD_LASTSAVED, 
            strm.PLD_MAXLENGTH, 
            strm.PLD_NUMBERTYPE, 
            strm.PLD_ONCHAINEDLOOKUP, 
            strm.PLD_ONCLASSCHANGE, 
            strm.PLD_ONLOOKUP, 
            strm.PLD_ONVALIDATE, 
            strm.PLD_OTHERATTRIBS, 
            strm.PLD_OTHERTAGS, 
            strm.PLD_PAGENAME, 
            strm.PLD_POSITIONINGROUP, 
            strm.PLD_PRESENTINJSP, 
            strm.PLD_RESPONSEXPATH, 
            strm.PLD_SIZE, 
            strm.PLD_SYSTEMATTRIBUTE, 
            strm.PLD_TABINDEX, 
            strm.PLD_UPDATECOUNT, 
            strm.PLD_XPATH, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PLD_PAGENAME,
            strm.PLD_ELEMENTID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DEFAULTPAGELAYOUT as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PLD_PAGENAME=src.PLD_PAGENAME) OR (target.PLD_PAGENAME IS NULL AND src.PLD_PAGENAME IS NULL)) AND
            ((target.PLD_ELEMENTID=src.PLD_ELEMENTID) OR (target.PLD_ELEMENTID IS NULL AND src.PLD_ELEMENTID IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.PLD_ATTRIBUTE=src.PLD_ATTRIBUTE, 
                    target.PLD_CASE=src.PLD_CASE, 
                    target.PLD_DDFIELDNAME=src.PLD_DDFIELDNAME, 
                    target.PLD_DDTABLENAME=src.PLD_DDTABLENAME, 
                    target.PLD_DESTXPATH=src.PLD_DESTXPATH, 
                    target.PLD_ELEMENTID=src.PLD_ELEMENTID, 
                    target.PLD_ELEMENTTYPE=src.PLD_ELEMENTTYPE, 
                    target.PLD_FIELDCONTAINER=src.PLD_FIELDCONTAINER, 
                    target.PLD_FIELDCONTTYPE=src.PLD_FIELDCONTTYPE, 
                    target.PLD_FIELDGROUP=src.PLD_FIELDGROUP, 
                    target.PLD_FIELDTYPE=src.PLD_FIELDTYPE, 
                    target.PLD_JSPFILE=src.PLD_JSPFILE, 
                    target.PLD_LASTSAVED=src.PLD_LASTSAVED, 
                    target.PLD_MAXLENGTH=src.PLD_MAXLENGTH, 
                    target.PLD_NUMBERTYPE=src.PLD_NUMBERTYPE, 
                    target.PLD_ONCHAINEDLOOKUP=src.PLD_ONCHAINEDLOOKUP, 
                    target.PLD_ONCLASSCHANGE=src.PLD_ONCLASSCHANGE, 
                    target.PLD_ONLOOKUP=src.PLD_ONLOOKUP, 
                    target.PLD_ONVALIDATE=src.PLD_ONVALIDATE, 
                    target.PLD_OTHERATTRIBS=src.PLD_OTHERATTRIBS, 
                    target.PLD_OTHERTAGS=src.PLD_OTHERTAGS, 
                    target.PLD_PAGENAME=src.PLD_PAGENAME, 
                    target.PLD_POSITIONINGROUP=src.PLD_POSITIONINGROUP, 
                    target.PLD_PRESENTINJSP=src.PLD_PRESENTINJSP, 
                    target.PLD_RESPONSEXPATH=src.PLD_RESPONSEXPATH, 
                    target.PLD_SIZE=src.PLD_SIZE, 
                    target.PLD_SYSTEMATTRIBUTE=src.PLD_SYSTEMATTRIBUTE, 
                    target.PLD_TABINDEX=src.PLD_TABINDEX, 
                    target.PLD_UPDATECOUNT=src.PLD_UPDATECOUNT, 
                    target.PLD_XPATH=src.PLD_XPATH, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    PLD_ATTRIBUTE, 
                    PLD_CASE, 
                    PLD_DDFIELDNAME, 
                    PLD_DDTABLENAME, 
                    PLD_DESTXPATH, 
                    PLD_ELEMENTID, 
                    PLD_ELEMENTTYPE, 
                    PLD_FIELDCONTAINER, 
                    PLD_FIELDCONTTYPE, 
                    PLD_FIELDGROUP, 
                    PLD_FIELDTYPE, 
                    PLD_JSPFILE, 
                    PLD_LASTSAVED, 
                    PLD_MAXLENGTH, 
                    PLD_NUMBERTYPE, 
                    PLD_ONCHAINEDLOOKUP, 
                    PLD_ONCLASSCHANGE, 
                    PLD_ONLOOKUP, 
                    PLD_ONVALIDATE, 
                    PLD_OTHERATTRIBS, 
                    PLD_OTHERTAGS, 
                    PLD_PAGENAME, 
                    PLD_POSITIONINGROUP, 
                    PLD_PRESENTINJSP, 
                    PLD_RESPONSEXPATH, 
                    PLD_SIZE, 
                    PLD_SYSTEMATTRIBUTE, 
                    PLD_TABINDEX, 
                    PLD_UPDATECOUNT, 
                    PLD_XPATH, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.PLD_ATTRIBUTE, 
                    src.PLD_CASE, 
                    src.PLD_DDFIELDNAME, 
                    src.PLD_DDTABLENAME, 
                    src.PLD_DESTXPATH, 
                    src.PLD_ELEMENTID, 
                    src.PLD_ELEMENTTYPE, 
                    src.PLD_FIELDCONTAINER, 
                    src.PLD_FIELDCONTTYPE, 
                    src.PLD_FIELDGROUP, 
                    src.PLD_FIELDTYPE, 
                    src.PLD_JSPFILE, 
                    src.PLD_LASTSAVED, 
                    src.PLD_MAXLENGTH, 
                    src.PLD_NUMBERTYPE, 
                    src.PLD_ONCHAINEDLOOKUP, 
                    src.PLD_ONCLASSCHANGE, 
                    src.PLD_ONLOOKUP, 
                    src.PLD_ONVALIDATE, 
                    src.PLD_OTHERATTRIBS, 
                    src.PLD_OTHERTAGS, 
                    src.PLD_PAGENAME, 
                    src.PLD_POSITIONINGROUP, 
                    src.PLD_PRESENTINJSP, 
                    src.PLD_RESPONSEXPATH, 
                    src.PLD_SIZE, 
                    src.PLD_SYSTEMATTRIBUTE, 
                    src.PLD_TABINDEX, 
                    src.PLD_UPDATECOUNT, 
                    src.PLD_XPATH, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5DEFAULTPAGELAYOUT_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.PLD_ATTRIBUTE, 
            strm.PLD_CASE, 
            strm.PLD_DDFIELDNAME, 
            strm.PLD_DDTABLENAME, 
            strm.PLD_DESTXPATH, 
            strm.PLD_ELEMENTID, 
            strm.PLD_ELEMENTTYPE, 
            strm.PLD_FIELDCONTAINER, 
            strm.PLD_FIELDCONTTYPE, 
            strm.PLD_FIELDGROUP, 
            strm.PLD_FIELDTYPE, 
            strm.PLD_JSPFILE, 
            strm.PLD_LASTSAVED, 
            strm.PLD_MAXLENGTH, 
            strm.PLD_NUMBERTYPE, 
            strm.PLD_ONCHAINEDLOOKUP, 
            strm.PLD_ONCLASSCHANGE, 
            strm.PLD_ONLOOKUP, 
            strm.PLD_ONVALIDATE, 
            strm.PLD_OTHERATTRIBS, 
            strm.PLD_OTHERTAGS, 
            strm.PLD_PAGENAME, 
            strm.PLD_POSITIONINGROUP, 
            strm.PLD_PRESENTINJSP, 
            strm.PLD_RESPONSEXPATH, 
            strm.PLD_SIZE, 
            strm.PLD_SYSTEMATTRIBUTE, 
            strm.PLD_TABINDEX, 
            strm.PLD_UPDATECOUNT, 
            strm.PLD_XPATH, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PLD_PAGENAME,
            strm.PLD_ELEMENTID ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DEFAULTPAGELAYOUT as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PLD_PAGENAME=src.PLD_PAGENAME) OR (target.PLD_PAGENAME IS NULL AND src.PLD_PAGENAME IS NULL)) AND
            ((target.PLD_ELEMENTID=src.PLD_ELEMENTID) OR (target.PLD_ELEMENTID IS NULL AND src.PLD_ELEMENTID IS NULL)) 
                when matched then update set
                    target.PLD_ATTRIBUTE=src.PLD_ATTRIBUTE, 
                    target.PLD_CASE=src.PLD_CASE, 
                    target.PLD_DDFIELDNAME=src.PLD_DDFIELDNAME, 
                    target.PLD_DDTABLENAME=src.PLD_DDTABLENAME, 
                    target.PLD_DESTXPATH=src.PLD_DESTXPATH, 
                    target.PLD_ELEMENTID=src.PLD_ELEMENTID, 
                    target.PLD_ELEMENTTYPE=src.PLD_ELEMENTTYPE, 
                    target.PLD_FIELDCONTAINER=src.PLD_FIELDCONTAINER, 
                    target.PLD_FIELDCONTTYPE=src.PLD_FIELDCONTTYPE, 
                    target.PLD_FIELDGROUP=src.PLD_FIELDGROUP, 
                    target.PLD_FIELDTYPE=src.PLD_FIELDTYPE, 
                    target.PLD_JSPFILE=src.PLD_JSPFILE, 
                    target.PLD_LASTSAVED=src.PLD_LASTSAVED, 
                    target.PLD_MAXLENGTH=src.PLD_MAXLENGTH, 
                    target.PLD_NUMBERTYPE=src.PLD_NUMBERTYPE, 
                    target.PLD_ONCHAINEDLOOKUP=src.PLD_ONCHAINEDLOOKUP, 
                    target.PLD_ONCLASSCHANGE=src.PLD_ONCLASSCHANGE, 
                    target.PLD_ONLOOKUP=src.PLD_ONLOOKUP, 
                    target.PLD_ONVALIDATE=src.PLD_ONVALIDATE, 
                    target.PLD_OTHERATTRIBS=src.PLD_OTHERATTRIBS, 
                    target.PLD_OTHERTAGS=src.PLD_OTHERTAGS, 
                    target.PLD_PAGENAME=src.PLD_PAGENAME, 
                    target.PLD_POSITIONINGROUP=src.PLD_POSITIONINGROUP, 
                    target.PLD_PRESENTINJSP=src.PLD_PRESENTINJSP, 
                    target.PLD_RESPONSEXPATH=src.PLD_RESPONSEXPATH, 
                    target.PLD_SIZE=src.PLD_SIZE, 
                    target.PLD_SYSTEMATTRIBUTE=src.PLD_SYSTEMATTRIBUTE, 
                    target.PLD_TABINDEX=src.PLD_TABINDEX, 
                    target.PLD_UPDATECOUNT=src.PLD_UPDATECOUNT, 
                    target.PLD_XPATH=src.PLD_XPATH, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( PLD_ATTRIBUTE, PLD_CASE, PLD_DDFIELDNAME, PLD_DDTABLENAME, PLD_DESTXPATH, PLD_ELEMENTID, PLD_ELEMENTTYPE, PLD_FIELDCONTAINER, PLD_FIELDCONTTYPE, PLD_FIELDGROUP, PLD_FIELDTYPE, PLD_JSPFILE, PLD_LASTSAVED, PLD_MAXLENGTH, PLD_NUMBERTYPE, PLD_ONCHAINEDLOOKUP, PLD_ONCLASSCHANGE, PLD_ONLOOKUP, PLD_ONVALIDATE, PLD_OTHERATTRIBS, PLD_OTHERTAGS, PLD_PAGENAME, PLD_POSITIONINGROUP, PLD_PRESENTINJSP, PLD_RESPONSEXPATH, PLD_SIZE, PLD_SYSTEMATTRIBUTE, PLD_TABINDEX, PLD_UPDATECOUNT, PLD_XPATH, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.PLD_ATTRIBUTE, 
                    src.PLD_CASE, 
                    src.PLD_DDFIELDNAME, 
                    src.PLD_DDTABLENAME, 
                    src.PLD_DESTXPATH, 
                    src.PLD_ELEMENTID, 
                    src.PLD_ELEMENTTYPE, 
                    src.PLD_FIELDCONTAINER, 
                    src.PLD_FIELDCONTTYPE, 
                    src.PLD_FIELDGROUP, 
                    src.PLD_FIELDTYPE, 
                    src.PLD_JSPFILE, 
                    src.PLD_LASTSAVED, 
                    src.PLD_MAXLENGTH, 
                    src.PLD_NUMBERTYPE, 
                    src.PLD_ONCHAINEDLOOKUP, 
                    src.PLD_ONCLASSCHANGE, 
                    src.PLD_ONLOOKUP, 
                    src.PLD_ONVALIDATE, 
                    src.PLD_OTHERATTRIBS, 
                    src.PLD_OTHERTAGS, 
                    src.PLD_PAGENAME, 
                    src.PLD_POSITIONINGROUP, 
                    src.PLD_PRESENTINJSP, 
                    src.PLD_RESPONSEXPATH, 
                    src.PLD_SIZE, 
                    src.PLD_SYSTEMATTRIBUTE, 
                    src.PLD_TABINDEX, 
                    src.PLD_UPDATECOUNT, 
                    src.PLD_XPATH, 
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
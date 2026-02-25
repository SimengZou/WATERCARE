create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5WSPROMPTFIELDS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5WSPROMPTFIELDS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5WSPROMPTFIELDS as target using (SELECT * FROM (SELECT 
            strm.WSF_BRANCHCOND, 
            strm.WSF_BRANCHGOTO, 
            strm.WSF_BRANCHPATTERN, 
            strm.WSF_CLEARPREVVALUES, 
            strm.WSF_COMPUTEDDATA, 
            strm.WSF_CUSTOMFIELDID, 
            strm.WSF_DESTWEBSERVICE, 
            strm.WSF_DESTXPATH, 
            strm.WSF_DISPLAYTYPE, 
            strm.WSF_DUPPREVVALUE, 
            strm.WSF_FIELD, 
            strm.WSF_FIELDLABEL, 
            strm.WSF_FIELDXPATH, 
            strm.WSF_FIXEDDATA, 
            strm.WSF_ISCATEGORY, 
            strm.WSF_ISCLASS, 
            strm.WSF_ISCLASSORG, 
            strm.WSF_ISCONTROLORG, 
            strm.WSF_LASTSAVED, 
            strm.WSF_LINE, 
            strm.WSF_MAXLENGTH, 
            strm.WSF_MINLENGTH, 
            strm.WSF_MOBILEQUERYCODE, 
            strm.WSF_NEXTLINE, 
            strm.WSF_PATTERN, 
            strm.WSF_PRECISION, 
            strm.WSF_PRIMARYKEY, 
            strm.WSF_PROCESSGROUP, 
            strm.WSF_RESPONSEXPATH, 
            strm.WSF_RETRIEVEFIELD, 
            strm.WSF_RETRIEVEFROMGROUP, 
            strm.WSF_RETRIEVEXPATH, 
            strm.WSF_RTYPE, 
            strm.WSF_SCALE, 
            strm.WSF_SQLQUERYCODE, 
            strm.WSF_SYSTEMFIELDTYPE, 
            strm.WSF_TAGNAME, 
            strm.WSF_TYPE, 
            strm.WSF_UNMAPPED, 
            strm.WSF_UPDATECOUNT, 
            strm.WSF_UPDATED, 
            strm.WSF_UPPERCASE, 
            strm.WSF_WSPMTCODE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.WSF_WSPMTCODE,
            strm.WSF_LINE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WSPROMPTFIELDS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.WSF_WSPMTCODE=src.WSF_WSPMTCODE) OR (target.WSF_WSPMTCODE IS NULL AND src.WSF_WSPMTCODE IS NULL)) AND
            ((target.WSF_LINE=src.WSF_LINE) OR (target.WSF_LINE IS NULL AND src.WSF_LINE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.WSF_BRANCHCOND=src.WSF_BRANCHCOND, 
                    target.WSF_BRANCHGOTO=src.WSF_BRANCHGOTO, 
                    target.WSF_BRANCHPATTERN=src.WSF_BRANCHPATTERN, 
                    target.WSF_CLEARPREVVALUES=src.WSF_CLEARPREVVALUES, 
                    target.WSF_COMPUTEDDATA=src.WSF_COMPUTEDDATA, 
                    target.WSF_CUSTOMFIELDID=src.WSF_CUSTOMFIELDID, 
                    target.WSF_DESTWEBSERVICE=src.WSF_DESTWEBSERVICE, 
                    target.WSF_DESTXPATH=src.WSF_DESTXPATH, 
                    target.WSF_DISPLAYTYPE=src.WSF_DISPLAYTYPE, 
                    target.WSF_DUPPREVVALUE=src.WSF_DUPPREVVALUE, 
                    target.WSF_FIELD=src.WSF_FIELD, 
                    target.WSF_FIELDLABEL=src.WSF_FIELDLABEL, 
                    target.WSF_FIELDXPATH=src.WSF_FIELDXPATH, 
                    target.WSF_FIXEDDATA=src.WSF_FIXEDDATA, 
                    target.WSF_ISCATEGORY=src.WSF_ISCATEGORY, 
                    target.WSF_ISCLASS=src.WSF_ISCLASS, 
                    target.WSF_ISCLASSORG=src.WSF_ISCLASSORG, 
                    target.WSF_ISCONTROLORG=src.WSF_ISCONTROLORG, 
                    target.WSF_LASTSAVED=src.WSF_LASTSAVED, 
                    target.WSF_LINE=src.WSF_LINE, 
                    target.WSF_MAXLENGTH=src.WSF_MAXLENGTH, 
                    target.WSF_MINLENGTH=src.WSF_MINLENGTH, 
                    target.WSF_MOBILEQUERYCODE=src.WSF_MOBILEQUERYCODE, 
                    target.WSF_NEXTLINE=src.WSF_NEXTLINE, 
                    target.WSF_PATTERN=src.WSF_PATTERN, 
                    target.WSF_PRECISION=src.WSF_PRECISION, 
                    target.WSF_PRIMARYKEY=src.WSF_PRIMARYKEY, 
                    target.WSF_PROCESSGROUP=src.WSF_PROCESSGROUP, 
                    target.WSF_RESPONSEXPATH=src.WSF_RESPONSEXPATH, 
                    target.WSF_RETRIEVEFIELD=src.WSF_RETRIEVEFIELD, 
                    target.WSF_RETRIEVEFROMGROUP=src.WSF_RETRIEVEFROMGROUP, 
                    target.WSF_RETRIEVEXPATH=src.WSF_RETRIEVEXPATH, 
                    target.WSF_RTYPE=src.WSF_RTYPE, 
                    target.WSF_SCALE=src.WSF_SCALE, 
                    target.WSF_SQLQUERYCODE=src.WSF_SQLQUERYCODE, 
                    target.WSF_SYSTEMFIELDTYPE=src.WSF_SYSTEMFIELDTYPE, 
                    target.WSF_TAGNAME=src.WSF_TAGNAME, 
                    target.WSF_TYPE=src.WSF_TYPE, 
                    target.WSF_UNMAPPED=src.WSF_UNMAPPED, 
                    target.WSF_UPDATECOUNT=src.WSF_UPDATECOUNT, 
                    target.WSF_UPDATED=src.WSF_UPDATED, 
                    target.WSF_UPPERCASE=src.WSF_UPPERCASE, 
                    target.WSF_WSPMTCODE=src.WSF_WSPMTCODE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    WSF_BRANCHCOND, 
                    WSF_BRANCHGOTO, 
                    WSF_BRANCHPATTERN, 
                    WSF_CLEARPREVVALUES, 
                    WSF_COMPUTEDDATA, 
                    WSF_CUSTOMFIELDID, 
                    WSF_DESTWEBSERVICE, 
                    WSF_DESTXPATH, 
                    WSF_DISPLAYTYPE, 
                    WSF_DUPPREVVALUE, 
                    WSF_FIELD, 
                    WSF_FIELDLABEL, 
                    WSF_FIELDXPATH, 
                    WSF_FIXEDDATA, 
                    WSF_ISCATEGORY, 
                    WSF_ISCLASS, 
                    WSF_ISCLASSORG, 
                    WSF_ISCONTROLORG, 
                    WSF_LASTSAVED, 
                    WSF_LINE, 
                    WSF_MAXLENGTH, 
                    WSF_MINLENGTH, 
                    WSF_MOBILEQUERYCODE, 
                    WSF_NEXTLINE, 
                    WSF_PATTERN, 
                    WSF_PRECISION, 
                    WSF_PRIMARYKEY, 
                    WSF_PROCESSGROUP, 
                    WSF_RESPONSEXPATH, 
                    WSF_RETRIEVEFIELD, 
                    WSF_RETRIEVEFROMGROUP, 
                    WSF_RETRIEVEXPATH, 
                    WSF_RTYPE, 
                    WSF_SCALE, 
                    WSF_SQLQUERYCODE, 
                    WSF_SYSTEMFIELDTYPE, 
                    WSF_TAGNAME, 
                    WSF_TYPE, 
                    WSF_UNMAPPED, 
                    WSF_UPDATECOUNT, 
                    WSF_UPDATED, 
                    WSF_UPPERCASE, 
                    WSF_WSPMTCODE, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.WSF_BRANCHCOND, 
                    src.WSF_BRANCHGOTO, 
                    src.WSF_BRANCHPATTERN, 
                    src.WSF_CLEARPREVVALUES, 
                    src.WSF_COMPUTEDDATA, 
                    src.WSF_CUSTOMFIELDID, 
                    src.WSF_DESTWEBSERVICE, 
                    src.WSF_DESTXPATH, 
                    src.WSF_DISPLAYTYPE, 
                    src.WSF_DUPPREVVALUE, 
                    src.WSF_FIELD, 
                    src.WSF_FIELDLABEL, 
                    src.WSF_FIELDXPATH, 
                    src.WSF_FIXEDDATA, 
                    src.WSF_ISCATEGORY, 
                    src.WSF_ISCLASS, 
                    src.WSF_ISCLASSORG, 
                    src.WSF_ISCONTROLORG, 
                    src.WSF_LASTSAVED, 
                    src.WSF_LINE, 
                    src.WSF_MAXLENGTH, 
                    src.WSF_MINLENGTH, 
                    src.WSF_MOBILEQUERYCODE, 
                    src.WSF_NEXTLINE, 
                    src.WSF_PATTERN, 
                    src.WSF_PRECISION, 
                    src.WSF_PRIMARYKEY, 
                    src.WSF_PROCESSGROUP, 
                    src.WSF_RESPONSEXPATH, 
                    src.WSF_RETRIEVEFIELD, 
                    src.WSF_RETRIEVEFROMGROUP, 
                    src.WSF_RETRIEVEXPATH, 
                    src.WSF_RTYPE, 
                    src.WSF_SCALE, 
                    src.WSF_SQLQUERYCODE, 
                    src.WSF_SYSTEMFIELDTYPE, 
                    src.WSF_TAGNAME, 
                    src.WSF_TYPE, 
                    src.WSF_UNMAPPED, 
                    src.WSF_UPDATECOUNT, 
                    src.WSF_UPDATED, 
                    src.WSF_UPPERCASE, 
                    src.WSF_WSPMTCODE, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5WSPROMPTFIELDS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.WSF_BRANCHCOND, 
            strm.WSF_BRANCHGOTO, 
            strm.WSF_BRANCHPATTERN, 
            strm.WSF_CLEARPREVVALUES, 
            strm.WSF_COMPUTEDDATA, 
            strm.WSF_CUSTOMFIELDID, 
            strm.WSF_DESTWEBSERVICE, 
            strm.WSF_DESTXPATH, 
            strm.WSF_DISPLAYTYPE, 
            strm.WSF_DUPPREVVALUE, 
            strm.WSF_FIELD, 
            strm.WSF_FIELDLABEL, 
            strm.WSF_FIELDXPATH, 
            strm.WSF_FIXEDDATA, 
            strm.WSF_ISCATEGORY, 
            strm.WSF_ISCLASS, 
            strm.WSF_ISCLASSORG, 
            strm.WSF_ISCONTROLORG, 
            strm.WSF_LASTSAVED, 
            strm.WSF_LINE, 
            strm.WSF_MAXLENGTH, 
            strm.WSF_MINLENGTH, 
            strm.WSF_MOBILEQUERYCODE, 
            strm.WSF_NEXTLINE, 
            strm.WSF_PATTERN, 
            strm.WSF_PRECISION, 
            strm.WSF_PRIMARYKEY, 
            strm.WSF_PROCESSGROUP, 
            strm.WSF_RESPONSEXPATH, 
            strm.WSF_RETRIEVEFIELD, 
            strm.WSF_RETRIEVEFROMGROUP, 
            strm.WSF_RETRIEVEXPATH, 
            strm.WSF_RTYPE, 
            strm.WSF_SCALE, 
            strm.WSF_SQLQUERYCODE, 
            strm.WSF_SYSTEMFIELDTYPE, 
            strm.WSF_TAGNAME, 
            strm.WSF_TYPE, 
            strm.WSF_UNMAPPED, 
            strm.WSF_UPDATECOUNT, 
            strm.WSF_UPDATED, 
            strm.WSF_UPPERCASE, 
            strm.WSF_WSPMTCODE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.WSF_WSPMTCODE,
            strm.WSF_LINE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WSPROMPTFIELDS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.WSF_WSPMTCODE=src.WSF_WSPMTCODE) OR (target.WSF_WSPMTCODE IS NULL AND src.WSF_WSPMTCODE IS NULL)) AND
            ((target.WSF_LINE=src.WSF_LINE) OR (target.WSF_LINE IS NULL AND src.WSF_LINE IS NULL)) 
                when matched then update set
                    target.WSF_BRANCHCOND=src.WSF_BRANCHCOND, 
                    target.WSF_BRANCHGOTO=src.WSF_BRANCHGOTO, 
                    target.WSF_BRANCHPATTERN=src.WSF_BRANCHPATTERN, 
                    target.WSF_CLEARPREVVALUES=src.WSF_CLEARPREVVALUES, 
                    target.WSF_COMPUTEDDATA=src.WSF_COMPUTEDDATA, 
                    target.WSF_CUSTOMFIELDID=src.WSF_CUSTOMFIELDID, 
                    target.WSF_DESTWEBSERVICE=src.WSF_DESTWEBSERVICE, 
                    target.WSF_DESTXPATH=src.WSF_DESTXPATH, 
                    target.WSF_DISPLAYTYPE=src.WSF_DISPLAYTYPE, 
                    target.WSF_DUPPREVVALUE=src.WSF_DUPPREVVALUE, 
                    target.WSF_FIELD=src.WSF_FIELD, 
                    target.WSF_FIELDLABEL=src.WSF_FIELDLABEL, 
                    target.WSF_FIELDXPATH=src.WSF_FIELDXPATH, 
                    target.WSF_FIXEDDATA=src.WSF_FIXEDDATA, 
                    target.WSF_ISCATEGORY=src.WSF_ISCATEGORY, 
                    target.WSF_ISCLASS=src.WSF_ISCLASS, 
                    target.WSF_ISCLASSORG=src.WSF_ISCLASSORG, 
                    target.WSF_ISCONTROLORG=src.WSF_ISCONTROLORG, 
                    target.WSF_LASTSAVED=src.WSF_LASTSAVED, 
                    target.WSF_LINE=src.WSF_LINE, 
                    target.WSF_MAXLENGTH=src.WSF_MAXLENGTH, 
                    target.WSF_MINLENGTH=src.WSF_MINLENGTH, 
                    target.WSF_MOBILEQUERYCODE=src.WSF_MOBILEQUERYCODE, 
                    target.WSF_NEXTLINE=src.WSF_NEXTLINE, 
                    target.WSF_PATTERN=src.WSF_PATTERN, 
                    target.WSF_PRECISION=src.WSF_PRECISION, 
                    target.WSF_PRIMARYKEY=src.WSF_PRIMARYKEY, 
                    target.WSF_PROCESSGROUP=src.WSF_PROCESSGROUP, 
                    target.WSF_RESPONSEXPATH=src.WSF_RESPONSEXPATH, 
                    target.WSF_RETRIEVEFIELD=src.WSF_RETRIEVEFIELD, 
                    target.WSF_RETRIEVEFROMGROUP=src.WSF_RETRIEVEFROMGROUP, 
                    target.WSF_RETRIEVEXPATH=src.WSF_RETRIEVEXPATH, 
                    target.WSF_RTYPE=src.WSF_RTYPE, 
                    target.WSF_SCALE=src.WSF_SCALE, 
                    target.WSF_SQLQUERYCODE=src.WSF_SQLQUERYCODE, 
                    target.WSF_SYSTEMFIELDTYPE=src.WSF_SYSTEMFIELDTYPE, 
                    target.WSF_TAGNAME=src.WSF_TAGNAME, 
                    target.WSF_TYPE=src.WSF_TYPE, 
                    target.WSF_UNMAPPED=src.WSF_UNMAPPED, 
                    target.WSF_UPDATECOUNT=src.WSF_UPDATECOUNT, 
                    target.WSF_UPDATED=src.WSF_UPDATED, 
                    target.WSF_UPPERCASE=src.WSF_UPPERCASE, 
                    target.WSF_WSPMTCODE=src.WSF_WSPMTCODE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( WSF_BRANCHCOND, WSF_BRANCHGOTO, WSF_BRANCHPATTERN, WSF_CLEARPREVVALUES, WSF_COMPUTEDDATA, WSF_CUSTOMFIELDID, WSF_DESTWEBSERVICE, WSF_DESTXPATH, WSF_DISPLAYTYPE, WSF_DUPPREVVALUE, WSF_FIELD, WSF_FIELDLABEL, WSF_FIELDXPATH, WSF_FIXEDDATA, WSF_ISCATEGORY, WSF_ISCLASS, WSF_ISCLASSORG, WSF_ISCONTROLORG, WSF_LASTSAVED, WSF_LINE, WSF_MAXLENGTH, WSF_MINLENGTH, WSF_MOBILEQUERYCODE, WSF_NEXTLINE, WSF_PATTERN, WSF_PRECISION, WSF_PRIMARYKEY, WSF_PROCESSGROUP, WSF_RESPONSEXPATH, WSF_RETRIEVEFIELD, WSF_RETRIEVEFROMGROUP, WSF_RETRIEVEXPATH, WSF_RTYPE, WSF_SCALE, WSF_SQLQUERYCODE, WSF_SYSTEMFIELDTYPE, WSF_TAGNAME, WSF_TYPE, WSF_UNMAPPED, WSF_UPDATECOUNT, WSF_UPDATED, WSF_UPPERCASE, WSF_WSPMTCODE, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.WSF_BRANCHCOND, 
                    src.WSF_BRANCHGOTO, 
                    src.WSF_BRANCHPATTERN, 
                    src.WSF_CLEARPREVVALUES, 
                    src.WSF_COMPUTEDDATA, 
                    src.WSF_CUSTOMFIELDID, 
                    src.WSF_DESTWEBSERVICE, 
                    src.WSF_DESTXPATH, 
                    src.WSF_DISPLAYTYPE, 
                    src.WSF_DUPPREVVALUE, 
                    src.WSF_FIELD, 
                    src.WSF_FIELDLABEL, 
                    src.WSF_FIELDXPATH, 
                    src.WSF_FIXEDDATA, 
                    src.WSF_ISCATEGORY, 
                    src.WSF_ISCLASS, 
                    src.WSF_ISCLASSORG, 
                    src.WSF_ISCONTROLORG, 
                    src.WSF_LASTSAVED, 
                    src.WSF_LINE, 
                    src.WSF_MAXLENGTH, 
                    src.WSF_MINLENGTH, 
                    src.WSF_MOBILEQUERYCODE, 
                    src.WSF_NEXTLINE, 
                    src.WSF_PATTERN, 
                    src.WSF_PRECISION, 
                    src.WSF_PRIMARYKEY, 
                    src.WSF_PROCESSGROUP, 
                    src.WSF_RESPONSEXPATH, 
                    src.WSF_RETRIEVEFIELD, 
                    src.WSF_RETRIEVEFROMGROUP, 
                    src.WSF_RETRIEVEXPATH, 
                    src.WSF_RTYPE, 
                    src.WSF_SCALE, 
                    src.WSF_SQLQUERYCODE, 
                    src.WSF_SYSTEMFIELDTYPE, 
                    src.WSF_TAGNAME, 
                    src.WSF_TYPE, 
                    src.WSF_UNMAPPED, 
                    src.WSF_UPDATECOUNT, 
                    src.WSF_UPDATED, 
                    src.WSF_UPPERCASE, 
                    src.WSF_WSPMTCODE, 
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
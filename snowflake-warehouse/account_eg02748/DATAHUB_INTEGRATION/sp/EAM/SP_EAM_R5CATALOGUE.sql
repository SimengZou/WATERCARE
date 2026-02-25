create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5CATALOGUE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5CATALOGUE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5CATALOGUE as target using (SELECT * FROM (SELECT 
            strm.CAT_COSTCODE, 
            strm.CAT_CURR, 
            strm.CAT_DATE, 
            strm.CAT_DESC, 
            strm.CAT_DOCUMOTO_BOOKID, 
            strm.CAT_DOCUMOTO_PART, 
            strm.CAT_EXCH, 
            strm.CAT_EXCHFROMDUAL, 
            strm.CAT_EXCHTODUAL, 
            strm.CAT_EXPPURPR, 
            strm.CAT_EXPQUOT, 
            strm.CAT_GROSS, 
            strm.CAT_INSERTOLDREFERENCE, 
            strm.CAT_IPERROR, 
            strm.CAT_IPERRORMESSAGE, 
            strm.CAT_IPLASTUPDATE, 
            strm.CAT_LASTSAVED, 
            strm.CAT_LEADTIME, 
            strm.CAT_MINORDQTY, 
            strm.CAT_MULTIPLY, 
            strm.CAT_PART, 
            strm.CAT_PART_ORG, 
            strm.CAT_PURPRICE, 
            strm.CAT_PURUOM, 
            strm.CAT_QUOTATION, 
            strm.CAT_QUOTPRICE, 
            strm.CAT_QUOTUOM, 
            strm.CAT_REF, 
            strm.CAT_REPPRICE, 
            strm.CAT_REPREF, 
            strm.CAT_SOURCECODE, 
            strm.CAT_SOURCESYSTEM, 
            strm.CAT_SUPPLIER, 
            strm.CAT_SUPPLIER_ORG, 
            strm.CAT_TAX, 
            strm.CAT_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CAT_PART,
            strm.CAT_PART_ORG,
            strm.CAT_SUPPLIER,
            strm.CAT_SUPPLIER_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CATALOGUE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CAT_PART=src.CAT_PART) OR (target.CAT_PART IS NULL AND src.CAT_PART IS NULL)) AND
            ((target.CAT_PART_ORG=src.CAT_PART_ORG) OR (target.CAT_PART_ORG IS NULL AND src.CAT_PART_ORG IS NULL)) AND
            ((target.CAT_SUPPLIER=src.CAT_SUPPLIER) OR (target.CAT_SUPPLIER IS NULL AND src.CAT_SUPPLIER IS NULL)) AND
            ((target.CAT_SUPPLIER_ORG=src.CAT_SUPPLIER_ORG) OR (target.CAT_SUPPLIER_ORG IS NULL AND src.CAT_SUPPLIER_ORG IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CAT_COSTCODE=src.CAT_COSTCODE, 
                    target.CAT_CURR=src.CAT_CURR, 
                    target.CAT_DATE=src.CAT_DATE, 
                    target.CAT_DESC=src.CAT_DESC, 
                    target.CAT_DOCUMOTO_BOOKID=src.CAT_DOCUMOTO_BOOKID, 
                    target.CAT_DOCUMOTO_PART=src.CAT_DOCUMOTO_PART, 
                    target.CAT_EXCH=src.CAT_EXCH, 
                    target.CAT_EXCHFROMDUAL=src.CAT_EXCHFROMDUAL, 
                    target.CAT_EXCHTODUAL=src.CAT_EXCHTODUAL, 
                    target.CAT_EXPPURPR=src.CAT_EXPPURPR, 
                    target.CAT_EXPQUOT=src.CAT_EXPQUOT, 
                    target.CAT_GROSS=src.CAT_GROSS, 
                    target.CAT_INSERTOLDREFERENCE=src.CAT_INSERTOLDREFERENCE, 
                    target.CAT_IPERROR=src.CAT_IPERROR, 
                    target.CAT_IPERRORMESSAGE=src.CAT_IPERRORMESSAGE, 
                    target.CAT_IPLASTUPDATE=src.CAT_IPLASTUPDATE, 
                    target.CAT_LASTSAVED=src.CAT_LASTSAVED, 
                    target.CAT_LEADTIME=src.CAT_LEADTIME, 
                    target.CAT_MINORDQTY=src.CAT_MINORDQTY, 
                    target.CAT_MULTIPLY=src.CAT_MULTIPLY, 
                    target.CAT_PART=src.CAT_PART, 
                    target.CAT_PART_ORG=src.CAT_PART_ORG, 
                    target.CAT_PURPRICE=src.CAT_PURPRICE, 
                    target.CAT_PURUOM=src.CAT_PURUOM, 
                    target.CAT_QUOTATION=src.CAT_QUOTATION, 
                    target.CAT_QUOTPRICE=src.CAT_QUOTPRICE, 
                    target.CAT_QUOTUOM=src.CAT_QUOTUOM, 
                    target.CAT_REF=src.CAT_REF, 
                    target.CAT_REPPRICE=src.CAT_REPPRICE, 
                    target.CAT_REPREF=src.CAT_REPREF, 
                    target.CAT_SOURCECODE=src.CAT_SOURCECODE, 
                    target.CAT_SOURCESYSTEM=src.CAT_SOURCESYSTEM, 
                    target.CAT_SUPPLIER=src.CAT_SUPPLIER, 
                    target.CAT_SUPPLIER_ORG=src.CAT_SUPPLIER_ORG, 
                    target.CAT_TAX=src.CAT_TAX, 
                    target.CAT_UPDATECOUNT=src.CAT_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CAT_COSTCODE, 
                    CAT_CURR, 
                    CAT_DATE, 
                    CAT_DESC, 
                    CAT_DOCUMOTO_BOOKID, 
                    CAT_DOCUMOTO_PART, 
                    CAT_EXCH, 
                    CAT_EXCHFROMDUAL, 
                    CAT_EXCHTODUAL, 
                    CAT_EXPPURPR, 
                    CAT_EXPQUOT, 
                    CAT_GROSS, 
                    CAT_INSERTOLDREFERENCE, 
                    CAT_IPERROR, 
                    CAT_IPERRORMESSAGE, 
                    CAT_IPLASTUPDATE, 
                    CAT_LASTSAVED, 
                    CAT_LEADTIME, 
                    CAT_MINORDQTY, 
                    CAT_MULTIPLY, 
                    CAT_PART, 
                    CAT_PART_ORG, 
                    CAT_PURPRICE, 
                    CAT_PURUOM, 
                    CAT_QUOTATION, 
                    CAT_QUOTPRICE, 
                    CAT_QUOTUOM, 
                    CAT_REF, 
                    CAT_REPPRICE, 
                    CAT_REPREF, 
                    CAT_SOURCECODE, 
                    CAT_SOURCESYSTEM, 
                    CAT_SUPPLIER, 
                    CAT_SUPPLIER_ORG, 
                    CAT_TAX, 
                    CAT_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CAT_COSTCODE, 
                    src.CAT_CURR, 
                    src.CAT_DATE, 
                    src.CAT_DESC, 
                    src.CAT_DOCUMOTO_BOOKID, 
                    src.CAT_DOCUMOTO_PART, 
                    src.CAT_EXCH, 
                    src.CAT_EXCHFROMDUAL, 
                    src.CAT_EXCHTODUAL, 
                    src.CAT_EXPPURPR, 
                    src.CAT_EXPQUOT, 
                    src.CAT_GROSS, 
                    src.CAT_INSERTOLDREFERENCE, 
                    src.CAT_IPERROR, 
                    src.CAT_IPERRORMESSAGE, 
                    src.CAT_IPLASTUPDATE, 
                    src.CAT_LASTSAVED, 
                    src.CAT_LEADTIME, 
                    src.CAT_MINORDQTY, 
                    src.CAT_MULTIPLY, 
                    src.CAT_PART, 
                    src.CAT_PART_ORG, 
                    src.CAT_PURPRICE, 
                    src.CAT_PURUOM, 
                    src.CAT_QUOTATION, 
                    src.CAT_QUOTPRICE, 
                    src.CAT_QUOTUOM, 
                    src.CAT_REF, 
                    src.CAT_REPPRICE, 
                    src.CAT_REPREF, 
                    src.CAT_SOURCECODE, 
                    src.CAT_SOURCESYSTEM, 
                    src.CAT_SUPPLIER, 
                    src.CAT_SUPPLIER_ORG, 
                    src.CAT_TAX, 
                    src.CAT_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5CATALOGUE_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CAT_COSTCODE, 
            strm.CAT_CURR, 
            strm.CAT_DATE, 
            strm.CAT_DESC, 
            strm.CAT_DOCUMOTO_BOOKID, 
            strm.CAT_DOCUMOTO_PART, 
            strm.CAT_EXCH, 
            strm.CAT_EXCHFROMDUAL, 
            strm.CAT_EXCHTODUAL, 
            strm.CAT_EXPPURPR, 
            strm.CAT_EXPQUOT, 
            strm.CAT_GROSS, 
            strm.CAT_INSERTOLDREFERENCE, 
            strm.CAT_IPERROR, 
            strm.CAT_IPERRORMESSAGE, 
            strm.CAT_IPLASTUPDATE, 
            strm.CAT_LASTSAVED, 
            strm.CAT_LEADTIME, 
            strm.CAT_MINORDQTY, 
            strm.CAT_MULTIPLY, 
            strm.CAT_PART, 
            strm.CAT_PART_ORG, 
            strm.CAT_PURPRICE, 
            strm.CAT_PURUOM, 
            strm.CAT_QUOTATION, 
            strm.CAT_QUOTPRICE, 
            strm.CAT_QUOTUOM, 
            strm.CAT_REF, 
            strm.CAT_REPPRICE, 
            strm.CAT_REPREF, 
            strm.CAT_SOURCECODE, 
            strm.CAT_SOURCESYSTEM, 
            strm.CAT_SUPPLIER, 
            strm.CAT_SUPPLIER_ORG, 
            strm.CAT_TAX, 
            strm.CAT_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CAT_PART,
            strm.CAT_PART_ORG,
            strm.CAT_SUPPLIER,
            strm.CAT_SUPPLIER_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CATALOGUE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CAT_PART=src.CAT_PART) OR (target.CAT_PART IS NULL AND src.CAT_PART IS NULL)) AND
            ((target.CAT_PART_ORG=src.CAT_PART_ORG) OR (target.CAT_PART_ORG IS NULL AND src.CAT_PART_ORG IS NULL)) AND
            ((target.CAT_SUPPLIER=src.CAT_SUPPLIER) OR (target.CAT_SUPPLIER IS NULL AND src.CAT_SUPPLIER IS NULL)) AND
            ((target.CAT_SUPPLIER_ORG=src.CAT_SUPPLIER_ORG) OR (target.CAT_SUPPLIER_ORG IS NULL AND src.CAT_SUPPLIER_ORG IS NULL)) 
                when matched then update set
                    target.CAT_COSTCODE=src.CAT_COSTCODE, 
                    target.CAT_CURR=src.CAT_CURR, 
                    target.CAT_DATE=src.CAT_DATE, 
                    target.CAT_DESC=src.CAT_DESC, 
                    target.CAT_DOCUMOTO_BOOKID=src.CAT_DOCUMOTO_BOOKID, 
                    target.CAT_DOCUMOTO_PART=src.CAT_DOCUMOTO_PART, 
                    target.CAT_EXCH=src.CAT_EXCH, 
                    target.CAT_EXCHFROMDUAL=src.CAT_EXCHFROMDUAL, 
                    target.CAT_EXCHTODUAL=src.CAT_EXCHTODUAL, 
                    target.CAT_EXPPURPR=src.CAT_EXPPURPR, 
                    target.CAT_EXPQUOT=src.CAT_EXPQUOT, 
                    target.CAT_GROSS=src.CAT_GROSS, 
                    target.CAT_INSERTOLDREFERENCE=src.CAT_INSERTOLDREFERENCE, 
                    target.CAT_IPERROR=src.CAT_IPERROR, 
                    target.CAT_IPERRORMESSAGE=src.CAT_IPERRORMESSAGE, 
                    target.CAT_IPLASTUPDATE=src.CAT_IPLASTUPDATE, 
                    target.CAT_LASTSAVED=src.CAT_LASTSAVED, 
                    target.CAT_LEADTIME=src.CAT_LEADTIME, 
                    target.CAT_MINORDQTY=src.CAT_MINORDQTY, 
                    target.CAT_MULTIPLY=src.CAT_MULTIPLY, 
                    target.CAT_PART=src.CAT_PART, 
                    target.CAT_PART_ORG=src.CAT_PART_ORG, 
                    target.CAT_PURPRICE=src.CAT_PURPRICE, 
                    target.CAT_PURUOM=src.CAT_PURUOM, 
                    target.CAT_QUOTATION=src.CAT_QUOTATION, 
                    target.CAT_QUOTPRICE=src.CAT_QUOTPRICE, 
                    target.CAT_QUOTUOM=src.CAT_QUOTUOM, 
                    target.CAT_REF=src.CAT_REF, 
                    target.CAT_REPPRICE=src.CAT_REPPRICE, 
                    target.CAT_REPREF=src.CAT_REPREF, 
                    target.CAT_SOURCECODE=src.CAT_SOURCECODE, 
                    target.CAT_SOURCESYSTEM=src.CAT_SOURCESYSTEM, 
                    target.CAT_SUPPLIER=src.CAT_SUPPLIER, 
                    target.CAT_SUPPLIER_ORG=src.CAT_SUPPLIER_ORG, 
                    target.CAT_TAX=src.CAT_TAX, 
                    target.CAT_UPDATECOUNT=src.CAT_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CAT_COSTCODE, CAT_CURR, CAT_DATE, CAT_DESC, CAT_DOCUMOTO_BOOKID, CAT_DOCUMOTO_PART, CAT_EXCH, CAT_EXCHFROMDUAL, CAT_EXCHTODUAL, CAT_EXPPURPR, CAT_EXPQUOT, CAT_GROSS, CAT_INSERTOLDREFERENCE, CAT_IPERROR, CAT_IPERRORMESSAGE, CAT_IPLASTUPDATE, CAT_LASTSAVED, CAT_LEADTIME, CAT_MINORDQTY, CAT_MULTIPLY, CAT_PART, CAT_PART_ORG, CAT_PURPRICE, CAT_PURUOM, CAT_QUOTATION, CAT_QUOTPRICE, CAT_QUOTUOM, CAT_REF, CAT_REPPRICE, CAT_REPREF, CAT_SOURCECODE, CAT_SOURCESYSTEM, CAT_SUPPLIER, CAT_SUPPLIER_ORG, CAT_TAX, CAT_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CAT_COSTCODE, 
                    src.CAT_CURR, 
                    src.CAT_DATE, 
                    src.CAT_DESC, 
                    src.CAT_DOCUMOTO_BOOKID, 
                    src.CAT_DOCUMOTO_PART, 
                    src.CAT_EXCH, 
                    src.CAT_EXCHFROMDUAL, 
                    src.CAT_EXCHTODUAL, 
                    src.CAT_EXPPURPR, 
                    src.CAT_EXPQUOT, 
                    src.CAT_GROSS, 
                    src.CAT_INSERTOLDREFERENCE, 
                    src.CAT_IPERROR, 
                    src.CAT_IPERRORMESSAGE, 
                    src.CAT_IPLASTUPDATE, 
                    src.CAT_LASTSAVED, 
                    src.CAT_LEADTIME, 
                    src.CAT_MINORDQTY, 
                    src.CAT_MULTIPLY, 
                    src.CAT_PART, 
                    src.CAT_PART_ORG, 
                    src.CAT_PURPRICE, 
                    src.CAT_PURUOM, 
                    src.CAT_QUOTATION, 
                    src.CAT_QUOTPRICE, 
                    src.CAT_QUOTUOM, 
                    src.CAT_REF, 
                    src.CAT_REPPRICE, 
                    src.CAT_REPREF, 
                    src.CAT_SOURCECODE, 
                    src.CAT_SOURCESYSTEM, 
                    src.CAT_SUPPLIER, 
                    src.CAT_SUPPLIER_ORG, 
                    src.CAT_TAX, 
                    src.CAT_UPDATECOUNT, 
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
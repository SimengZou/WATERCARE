create or replace procedure DATAHUB_INTEGRATION.SP_IPS_METERMANAGEMENT_WATER_READINGEXPORTSETUP()
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
                            INSERT INTO LANDING_ERROR.IPS_RAW_ERROR
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_METERMANAGEMENT_WATER_READINGEXPORTSETUP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_METERMANAGEMENT_WATER_READINGEXPORTSETUP as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ALLOWARCHIVEFLAG, 
            strm.ALLOWOUTSTANDINGROUTESFLAG, 
            strm.CONCATENATEFILEFLAG, 
            strm.DAYSBEFORENEXTREADINGDAY, 
            strm.DELETED, 
            strm.DELETEPREVFILEVERFLAG, 
            strm.EXCLBILLEDDAYS, 
            strm.EXCLBILLEDFLAG, 
            strm.EXCLEXCEPMETERS, 
            strm.EXCLLASTREADDAYS, 
            strm.EXCLLASTREADFLAG, 
            strm.EXCLNEWMETERS, 
            strm.EXCLNOACCTMETERS, 
            strm.EXCLSRAFTERDAYS, 
            strm.EXCLSRBEFOREDAYS, 
            strm.EXCLSRFLAG, 
            strm.EXCLZEROREADINGMETERS, 
            strm.EXCLZEROUSAGEMETERS, 
            strm.EXPORTDIRPATH, 
            strm.EXPORTRUNNUMBER, 
            strm.EXPORTTEMPLATEFILE, 
            strm.EXPORTTYPE, 
            strm.FILENUMBERCOUNTER, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.READINGEXPORTSETUPKEY, 
            strm.RECEIVERCODE, 
            strm.SENDERCODE, 
            strm.SORTBYCOLUMNS, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.READINGEXPORTSETUPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_METERMANAGEMENT_WATER_READINGEXPORTSETUP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.READINGEXPORTSETUPKEY=src.READINGEXPORTSETUPKEY) OR (target.READINGEXPORTSETUPKEY IS NULL AND src.READINGEXPORTSETUPKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ALLOWARCHIVEFLAG=src.ALLOWARCHIVEFLAG, 
                    target.ALLOWOUTSTANDINGROUTESFLAG=src.ALLOWOUTSTANDINGROUTESFLAG, 
                    target.CONCATENATEFILEFLAG=src.CONCATENATEFILEFLAG, 
                    target.DAYSBEFORENEXTREADINGDAY=src.DAYSBEFORENEXTREADINGDAY, 
                    target.DELETED=src.DELETED, 
                    target.DELETEPREVFILEVERFLAG=src.DELETEPREVFILEVERFLAG, 
                    target.EXCLBILLEDDAYS=src.EXCLBILLEDDAYS, 
                    target.EXCLBILLEDFLAG=src.EXCLBILLEDFLAG, 
                    target.EXCLEXCEPMETERS=src.EXCLEXCEPMETERS, 
                    target.EXCLLASTREADDAYS=src.EXCLLASTREADDAYS, 
                    target.EXCLLASTREADFLAG=src.EXCLLASTREADFLAG, 
                    target.EXCLNEWMETERS=src.EXCLNEWMETERS, 
                    target.EXCLNOACCTMETERS=src.EXCLNOACCTMETERS, 
                    target.EXCLSRAFTERDAYS=src.EXCLSRAFTERDAYS, 
                    target.EXCLSRBEFOREDAYS=src.EXCLSRBEFOREDAYS, 
                    target.EXCLSRFLAG=src.EXCLSRFLAG, 
                    target.EXCLZEROREADINGMETERS=src.EXCLZEROREADINGMETERS, 
                    target.EXCLZEROUSAGEMETERS=src.EXCLZEROUSAGEMETERS, 
                    target.EXPORTDIRPATH=src.EXPORTDIRPATH, 
                    target.EXPORTRUNNUMBER=src.EXPORTRUNNUMBER, 
                    target.EXPORTTEMPLATEFILE=src.EXPORTTEMPLATEFILE, 
                    target.EXPORTTYPE=src.EXPORTTYPE, 
                    target.FILENUMBERCOUNTER=src.FILENUMBERCOUNTER, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.READINGEXPORTSETUPKEY=src.READINGEXPORTSETUPKEY, 
                    target.RECEIVERCODE=src.RECEIVERCODE, 
                    target.SENDERCODE=src.SENDERCODE, 
                    target.SORTBYCOLUMNS=src.SORTBYCOLUMNS, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    ALLOWARCHIVEFLAG, 
                    ALLOWOUTSTANDINGROUTESFLAG, 
                    CONCATENATEFILEFLAG, 
                    DAYSBEFORENEXTREADINGDAY, 
                    DELETED, 
                    DELETEPREVFILEVERFLAG, 
                    EXCLBILLEDDAYS, 
                    EXCLBILLEDFLAG, 
                    EXCLEXCEPMETERS, 
                    EXCLLASTREADDAYS, 
                    EXCLLASTREADFLAG, 
                    EXCLNEWMETERS, 
                    EXCLNOACCTMETERS, 
                    EXCLSRAFTERDAYS, 
                    EXCLSRBEFOREDAYS, 
                    EXCLSRFLAG, 
                    EXCLZEROREADINGMETERS, 
                    EXCLZEROUSAGEMETERS, 
                    EXPORTDIRPATH, 
                    EXPORTRUNNUMBER, 
                    EXPORTTEMPLATEFILE, 
                    EXPORTTYPE, 
                    FILENUMBERCOUNTER, 
                    MODBY, 
                    MODDTTM, 
                    READINGEXPORTSETUPKEY, 
                    RECEIVERCODE, 
                    SENDERCODE, 
                    SORTBYCOLUMNS, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ALLOWARCHIVEFLAG, 
                    src.ALLOWOUTSTANDINGROUTESFLAG, 
                    src.CONCATENATEFILEFLAG, 
                    src.DAYSBEFORENEXTREADINGDAY, 
                    src.DELETED, 
                    src.DELETEPREVFILEVERFLAG, 
                    src.EXCLBILLEDDAYS, 
                    src.EXCLBILLEDFLAG, 
                    src.EXCLEXCEPMETERS, 
                    src.EXCLLASTREADDAYS, 
                    src.EXCLLASTREADFLAG, 
                    src.EXCLNEWMETERS, 
                    src.EXCLNOACCTMETERS, 
                    src.EXCLSRAFTERDAYS, 
                    src.EXCLSRBEFOREDAYS, 
                    src.EXCLSRFLAG, 
                    src.EXCLZEROREADINGMETERS, 
                    src.EXCLZEROUSAGEMETERS, 
                    src.EXPORTDIRPATH, 
                    src.EXPORTRUNNUMBER, 
                    src.EXPORTTEMPLATEFILE, 
                    src.EXPORTTYPE, 
                    src.FILENUMBERCOUNTER, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.READINGEXPORTSETUPKEY, 
                    src.RECEIVERCODE, 
                    src.SENDERCODE, 
                    src.SORTBYCOLUMNS, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_METERMANAGEMENT_WATER_READINGEXPORTSETUP as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ALLOWARCHIVEFLAG, 
            strm.ALLOWOUTSTANDINGROUTESFLAG, 
            strm.CONCATENATEFILEFLAG, 
            strm.DAYSBEFORENEXTREADINGDAY, 
            strm.DELETED, 
            strm.DELETEPREVFILEVERFLAG, 
            strm.EXCLBILLEDDAYS, 
            strm.EXCLBILLEDFLAG, 
            strm.EXCLEXCEPMETERS, 
            strm.EXCLLASTREADDAYS, 
            strm.EXCLLASTREADFLAG, 
            strm.EXCLNEWMETERS, 
            strm.EXCLNOACCTMETERS, 
            strm.EXCLSRAFTERDAYS, 
            strm.EXCLSRBEFOREDAYS, 
            strm.EXCLSRFLAG, 
            strm.EXCLZEROREADINGMETERS, 
            strm.EXCLZEROUSAGEMETERS, 
            strm.EXPORTDIRPATH, 
            strm.EXPORTRUNNUMBER, 
            strm.EXPORTTEMPLATEFILE, 
            strm.EXPORTTYPE, 
            strm.FILENUMBERCOUNTER, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.READINGEXPORTSETUPKEY, 
            strm.RECEIVERCODE, 
            strm.SENDERCODE, 
            strm.SORTBYCOLUMNS, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.READINGEXPORTSETUPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_METERMANAGEMENT_WATER_READINGEXPORTSETUP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.READINGEXPORTSETUPKEY=src.READINGEXPORTSETUPKEY) OR (target.READINGEXPORTSETUPKEY IS NULL AND src.READINGEXPORTSETUPKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ALLOWARCHIVEFLAG=src.ALLOWARCHIVEFLAG, 
                    target.ALLOWOUTSTANDINGROUTESFLAG=src.ALLOWOUTSTANDINGROUTESFLAG, 
                    target.CONCATENATEFILEFLAG=src.CONCATENATEFILEFLAG, 
                    target.DAYSBEFORENEXTREADINGDAY=src.DAYSBEFORENEXTREADINGDAY, 
                    target.DELETED=src.DELETED, 
                    target.DELETEPREVFILEVERFLAG=src.DELETEPREVFILEVERFLAG, 
                    target.EXCLBILLEDDAYS=src.EXCLBILLEDDAYS, 
                    target.EXCLBILLEDFLAG=src.EXCLBILLEDFLAG, 
                    target.EXCLEXCEPMETERS=src.EXCLEXCEPMETERS, 
                    target.EXCLLASTREADDAYS=src.EXCLLASTREADDAYS, 
                    target.EXCLLASTREADFLAG=src.EXCLLASTREADFLAG, 
                    target.EXCLNEWMETERS=src.EXCLNEWMETERS, 
                    target.EXCLNOACCTMETERS=src.EXCLNOACCTMETERS, 
                    target.EXCLSRAFTERDAYS=src.EXCLSRAFTERDAYS, 
                    target.EXCLSRBEFOREDAYS=src.EXCLSRBEFOREDAYS, 
                    target.EXCLSRFLAG=src.EXCLSRFLAG, 
                    target.EXCLZEROREADINGMETERS=src.EXCLZEROREADINGMETERS, 
                    target.EXCLZEROUSAGEMETERS=src.EXCLZEROUSAGEMETERS, 
                    target.EXPORTDIRPATH=src.EXPORTDIRPATH, 
                    target.EXPORTRUNNUMBER=src.EXPORTRUNNUMBER, 
                    target.EXPORTTEMPLATEFILE=src.EXPORTTEMPLATEFILE, 
                    target.EXPORTTYPE=src.EXPORTTYPE, 
                    target.FILENUMBERCOUNTER=src.FILENUMBERCOUNTER, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.READINGEXPORTSETUPKEY=src.READINGEXPORTSETUPKEY, 
                    target.RECEIVERCODE=src.RECEIVERCODE, 
                    target.SENDERCODE=src.SENDERCODE, 
                    target.SORTBYCOLUMNS=src.SORTBYCOLUMNS, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, ALLOWARCHIVEFLAG, ALLOWOUTSTANDINGROUTESFLAG, CONCATENATEFILEFLAG, DAYSBEFORENEXTREADINGDAY, DELETED, DELETEPREVFILEVERFLAG, EXCLBILLEDDAYS, EXCLBILLEDFLAG, EXCLEXCEPMETERS, EXCLLASTREADDAYS, EXCLLASTREADFLAG, EXCLNEWMETERS, EXCLNOACCTMETERS, EXCLSRAFTERDAYS, EXCLSRBEFOREDAYS, EXCLSRFLAG, EXCLZEROREADINGMETERS, EXCLZEROUSAGEMETERS, EXPORTDIRPATH, EXPORTRUNNUMBER, EXPORTTEMPLATEFILE, EXPORTTYPE, FILENUMBERCOUNTER, MODBY, MODDTTM, READINGEXPORTSETUPKEY, RECEIVERCODE, SENDERCODE, SORTBYCOLUMNS, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ALLOWARCHIVEFLAG, 
                    src.ALLOWOUTSTANDINGROUTESFLAG, 
                    src.CONCATENATEFILEFLAG, 
                    src.DAYSBEFORENEXTREADINGDAY, 
                    src.DELETED, 
                    src.DELETEPREVFILEVERFLAG, 
                    src.EXCLBILLEDDAYS, 
                    src.EXCLBILLEDFLAG, 
                    src.EXCLEXCEPMETERS, 
                    src.EXCLLASTREADDAYS, 
                    src.EXCLLASTREADFLAG, 
                    src.EXCLNEWMETERS, 
                    src.EXCLNOACCTMETERS, 
                    src.EXCLSRAFTERDAYS, 
                    src.EXCLSRBEFOREDAYS, 
                    src.EXCLSRFLAG, 
                    src.EXCLZEROREADINGMETERS, 
                    src.EXCLZEROUSAGEMETERS, 
                    src.EXPORTDIRPATH, 
                    src.EXPORTRUNNUMBER, 
                    src.EXPORTTEMPLATEFILE, 
                    src.EXPORTTYPE, 
                    src.FILENUMBERCOUNTER, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.READINGEXPORTSETUPKEY, 
                    src.RECEIVERCODE, 
                    src.SENDERCODE, 
                    src.SORTBYCOLUMNS, 
                    src.VARIATION_ID,     
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
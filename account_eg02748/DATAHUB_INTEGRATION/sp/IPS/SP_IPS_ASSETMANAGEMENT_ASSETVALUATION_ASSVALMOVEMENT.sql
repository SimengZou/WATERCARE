create or replace procedure DATAHUB_INTEGRATION.SP_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALMOVEMENT()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALMOVEMENT_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALMOVEMENT as target using (SELECT * FROM (SELECT 
            strm.ACCUMDEPRECPE, 
            strm.ACCUMDEPRECPS, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ASSVALKEY, 
            strm.ASSVALMOVEMENTKEY, 
            strm.CAPTCOSTCP, 
            strm.CURRENTCOSTPE, 
            strm.CURRENTCOSTPS, 
            strm.DATALAKE_DELETED, 
            strm.DEPRECCP, 
            strm.DISPDEPRECWRTNBCKCP, 
            strm.DISPOSALSCD, 
            strm.EFFECTIVEDTTM, 
            strm.IMPVALPRIORTOREVALPE, 
            strm.INITCOSTCP, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PLONSALECP, 
            strm.PROCONDISPCP, 
            strm.REVALACCUMDEPRECBGHTTOACCTCP, 
            strm.REVALACCUMDEPRECWRTNBCKGROSSCP, 
            strm.REVALACCUMDEPRECWRTNBCKNETCP, 
            strm.REVALINCDEC, 
            strm.VARIATION_ID, 
            strm.WDVPE, 
            strm.WDVPS, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ASSVALMOVEMENTKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALMOVEMENT as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ASSVALMOVEMENTKEY=src.ASSVALMOVEMENTKEY) OR (target.ASSVALMOVEMENTKEY IS NULL AND src.ASSVALMOVEMENTKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCUMDEPRECPE=src.ACCUMDEPRECPE, 
                    target.ACCUMDEPRECPS=src.ACCUMDEPRECPS, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ASSVALKEY=src.ASSVALKEY, 
                    target.ASSVALMOVEMENTKEY=src.ASSVALMOVEMENTKEY, 
                    target.CAPTCOSTCP=src.CAPTCOSTCP, 
                    target.CURRENTCOSTPE=src.CURRENTCOSTPE, 
                    target.CURRENTCOSTPS=src.CURRENTCOSTPS, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DEPRECCP=src.DEPRECCP, 
                    target.DISPDEPRECWRTNBCKCP=src.DISPDEPRECWRTNBCKCP, 
                    target.DISPOSALSCD=src.DISPOSALSCD, 
                    target.EFFECTIVEDTTM=src.EFFECTIVEDTTM, 
                    target.IMPVALPRIORTOREVALPE=src.IMPVALPRIORTOREVALPE, 
                    target.INITCOSTCP=src.INITCOSTCP, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PLONSALECP=src.PLONSALECP, 
                    target.PROCONDISPCP=src.PROCONDISPCP, 
                    target.REVALACCUMDEPRECBGHTTOACCTCP=src.REVALACCUMDEPRECBGHTTOACCTCP, 
                    target.REVALACCUMDEPRECWRTNBCKGROSSCP=src.REVALACCUMDEPRECWRTNBCKGROSSCP, 
                    target.REVALACCUMDEPRECWRTNBCKNETCP=src.REVALACCUMDEPRECWRTNBCKNETCP, 
                    target.REVALINCDEC=src.REVALINCDEC, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WDVPE=src.WDVPE, 
                    target.WDVPS=src.WDVPS, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCUMDEPRECPE, 
                    ACCUMDEPRECPS, 
                    ADDBY, 
                    ADDDTTM, 
                    ASSVALKEY, 
                    ASSVALMOVEMENTKEY, 
                    CAPTCOSTCP, 
                    CURRENTCOSTPE, 
                    CURRENTCOSTPS, 
                    DATALAKE_DELETED, 
                    DEPRECCP, 
                    DISPDEPRECWRTNBCKCP, 
                    DISPOSALSCD, 
                    EFFECTIVEDTTM, 
                    IMPVALPRIORTOREVALPE, 
                    INITCOSTCP, 
                    MODBY, 
                    MODDTTM, 
                    PLONSALECP, 
                    PROCONDISPCP, 
                    REVALACCUMDEPRECBGHTTOACCTCP, 
                    REVALACCUMDEPRECWRTNBCKGROSSCP, 
                    REVALACCUMDEPRECWRTNBCKNETCP, 
                    REVALINCDEC, 
                    VARIATION_ID, 
                    WDVPE, 
                    WDVPS, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCUMDEPRECPE, 
                    src.ACCUMDEPRECPS, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ASSVALKEY, 
                    src.ASSVALMOVEMENTKEY, 
                    src.CAPTCOSTCP, 
                    src.CURRENTCOSTPE, 
                    src.CURRENTCOSTPS, 
                    src.DATALAKE_DELETED, 
                    src.DEPRECCP, 
                    src.DISPDEPRECWRTNBCKCP, 
                    src.DISPOSALSCD, 
                    src.EFFECTIVEDTTM, 
                    src.IMPVALPRIORTOREVALPE, 
                    src.INITCOSTCP, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PLONSALECP, 
                    src.PROCONDISPCP, 
                    src.REVALACCUMDEPRECBGHTTOACCTCP, 
                    src.REVALACCUMDEPRECWRTNBCKGROSSCP, 
                    src.REVALACCUMDEPRECWRTNBCKNETCP, 
                    src.REVALINCDEC, 
                    src.VARIATION_ID, 
                    src.WDVPE, 
                    src.WDVPS,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_ASSETMANAGEMENT_ASSETVALUATION_ASSVALMOVEMENT as target using (
                SELECT * FROM (SELECT 
            strm.ACCUMDEPRECPE, 
            strm.ACCUMDEPRECPS, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ASSVALKEY, 
            strm.ASSVALMOVEMENTKEY, 
            strm.CAPTCOSTCP, 
            strm.CURRENTCOSTPE, 
            strm.CURRENTCOSTPS, 
            strm.DATALAKE_DELETED, 
            strm.DEPRECCP, 
            strm.DISPDEPRECWRTNBCKCP, 
            strm.DISPOSALSCD, 
            strm.EFFECTIVEDTTM, 
            strm.IMPVALPRIORTOREVALPE, 
            strm.INITCOSTCP, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PLONSALECP, 
            strm.PROCONDISPCP, 
            strm.REVALACCUMDEPRECBGHTTOACCTCP, 
            strm.REVALACCUMDEPRECWRTNBCKGROSSCP, 
            strm.REVALACCUMDEPRECWRTNBCKNETCP, 
            strm.REVALINCDEC, 
            strm.VARIATION_ID, 
            strm.WDVPE, 
            strm.WDVPS, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ASSVALMOVEMENTKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALMOVEMENT as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ASSVALMOVEMENTKEY=src.ASSVALMOVEMENTKEY) OR (target.ASSVALMOVEMENTKEY IS NULL AND src.ASSVALMOVEMENTKEY IS NULL)) 
                when matched then update set
                    target.ACCUMDEPRECPE=src.ACCUMDEPRECPE, 
                    target.ACCUMDEPRECPS=src.ACCUMDEPRECPS, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ASSVALKEY=src.ASSVALKEY, 
                    target.ASSVALMOVEMENTKEY=src.ASSVALMOVEMENTKEY, 
                    target.CAPTCOSTCP=src.CAPTCOSTCP, 
                    target.CURRENTCOSTPE=src.CURRENTCOSTPE, 
                    target.CURRENTCOSTPS=src.CURRENTCOSTPS, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DEPRECCP=src.DEPRECCP, 
                    target.DISPDEPRECWRTNBCKCP=src.DISPDEPRECWRTNBCKCP, 
                    target.DISPOSALSCD=src.DISPOSALSCD, 
                    target.EFFECTIVEDTTM=src.EFFECTIVEDTTM, 
                    target.IMPVALPRIORTOREVALPE=src.IMPVALPRIORTOREVALPE, 
                    target.INITCOSTCP=src.INITCOSTCP, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PLONSALECP=src.PLONSALECP, 
                    target.PROCONDISPCP=src.PROCONDISPCP, 
                    target.REVALACCUMDEPRECBGHTTOACCTCP=src.REVALACCUMDEPRECBGHTTOACCTCP, 
                    target.REVALACCUMDEPRECWRTNBCKGROSSCP=src.REVALACCUMDEPRECWRTNBCKGROSSCP, 
                    target.REVALACCUMDEPRECWRTNBCKNETCP=src.REVALACCUMDEPRECWRTNBCKNETCP, 
                    target.REVALINCDEC=src.REVALINCDEC, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WDVPE=src.WDVPE, 
                    target.WDVPS=src.WDVPS, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCUMDEPRECPE, ACCUMDEPRECPS, ADDBY, ADDDTTM, ASSVALKEY, ASSVALMOVEMENTKEY, CAPTCOSTCP, CURRENTCOSTPE, CURRENTCOSTPS, DATALAKE_DELETED, DEPRECCP, DISPDEPRECWRTNBCKCP, DISPOSALSCD, EFFECTIVEDTTM, IMPVALPRIORTOREVALPE, INITCOSTCP, MODBY, MODDTTM, PLONSALECP, PROCONDISPCP, REVALACCUMDEPRECBGHTTOACCTCP, REVALACCUMDEPRECWRTNBCKGROSSCP, REVALACCUMDEPRECWRTNBCKNETCP, REVALINCDEC, VARIATION_ID, WDVPE, WDVPS, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCUMDEPRECPE, 
                    src.ACCUMDEPRECPS, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ASSVALKEY, 
                    src.ASSVALMOVEMENTKEY, 
                    src.CAPTCOSTCP, 
                    src.CURRENTCOSTPE, 
                    src.CURRENTCOSTPS, 
                    src.DATALAKE_DELETED, 
                    src.DEPRECCP, 
                    src.DISPDEPRECWRTNBCKCP, 
                    src.DISPOSALSCD, 
                    src.EFFECTIVEDTTM, 
                    src.IMPVALPRIORTOREVALPE, 
                    src.INITCOSTCP, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PLONSALECP, 
                    src.PROCONDISPCP, 
                    src.REVALACCUMDEPRECBGHTTOACCTCP, 
                    src.REVALACCUMDEPRECWRTNBCKGROSSCP, 
                    src.REVALACCUMDEPRECWRTNBCKNETCP, 
                    src.REVALINCDEC, 
                    src.VARIATION_ID, 
                    src.WDVPE, 
                    src.WDVPS,     
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
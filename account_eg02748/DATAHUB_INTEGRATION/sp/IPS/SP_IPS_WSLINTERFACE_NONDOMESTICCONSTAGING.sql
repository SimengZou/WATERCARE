create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLINTERFACE_NONDOMESTICCONSTAGING()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLINTERFACE_NONDOMESTICCONSTAGING_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLINTERFACE_NONDOMESTICCONSTAGING as target using (SELECT * FROM (SELECT 
            strm.ACTIONTYPE, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APBLDGINSPKEY, 
            strm.APBLDGKEY, 
            strm.APNO, 
            strm.APPLICATIONTYPE, 
            strm.CONTRACTOR, 
            strm.DATALAKE_DELETED, 
            strm.DP, 
            strm.ENGINEERFULLNAME, 
            strm.FLAT, 
            strm.HOUSENUMBER, 
            strm.LEGALOWNEREMAIL, 
            strm.LEGALOWNERFULLNAME, 
            strm.LEGALOWNERMOBILE, 
            strm.LOT, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NONDOMESTICCONSTAGINGKEY, 
            strm.ONSITEEMAIL, 
            strm.ONSITEFULLNAME, 
            strm.ONSITEMOBILE, 
            strm.POSTALCODE, 
            strm.REQUESTTYPE, 
            strm.STREETNAME, 
            strm.STREETTYPE, 
            strm.SUBURB, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.NONDOMESTICCONSTAGINGKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLINTERFACE_NONDOMESTICCONSTAGING as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.NONDOMESTICCONSTAGINGKEY=src.NONDOMESTICCONSTAGINGKEY) OR (target.NONDOMESTICCONSTAGINGKEY IS NULL AND src.NONDOMESTICCONSTAGINGKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACTIONTYPE=src.ACTIONTYPE, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APBLDGINSPKEY=src.APBLDGINSPKEY, 
                    target.APBLDGKEY=src.APBLDGKEY, 
                    target.APNO=src.APNO, 
                    target.APPLICATIONTYPE=src.APPLICATIONTYPE, 
                    target.CONTRACTOR=src.CONTRACTOR, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DP=src.DP, 
                    target.ENGINEERFULLNAME=src.ENGINEERFULLNAME, 
                    target.FLAT=src.FLAT, 
                    target.HOUSENUMBER=src.HOUSENUMBER, 
                    target.LEGALOWNEREMAIL=src.LEGALOWNEREMAIL, 
                    target.LEGALOWNERFULLNAME=src.LEGALOWNERFULLNAME, 
                    target.LEGALOWNERMOBILE=src.LEGALOWNERMOBILE, 
                    target.LOT=src.LOT, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NONDOMESTICCONSTAGINGKEY=src.NONDOMESTICCONSTAGINGKEY, 
                    target.ONSITEEMAIL=src.ONSITEEMAIL, 
                    target.ONSITEFULLNAME=src.ONSITEFULLNAME, 
                    target.ONSITEMOBILE=src.ONSITEMOBILE, 
                    target.POSTALCODE=src.POSTALCODE, 
                    target.REQUESTTYPE=src.REQUESTTYPE, 
                    target.STREETNAME=src.STREETNAME, 
                    target.STREETTYPE=src.STREETTYPE, 
                    target.SUBURB=src.SUBURB, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACTIONTYPE, 
                    ADDBY, 
                    ADDDTTM, 
                    APBLDGINSPKEY, 
                    APBLDGKEY, 
                    APNO, 
                    APPLICATIONTYPE, 
                    CONTRACTOR, 
                    DATALAKE_DELETED, 
                    DP, 
                    ENGINEERFULLNAME, 
                    FLAT, 
                    HOUSENUMBER, 
                    LEGALOWNEREMAIL, 
                    LEGALOWNERFULLNAME, 
                    LEGALOWNERMOBILE, 
                    LOT, 
                    MODBY, 
                    MODDTTM, 
                    NONDOMESTICCONSTAGINGKEY, 
                    ONSITEEMAIL, 
                    ONSITEFULLNAME, 
                    ONSITEMOBILE, 
                    POSTALCODE, 
                    REQUESTTYPE, 
                    STREETNAME, 
                    STREETTYPE, 
                    SUBURB, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACTIONTYPE, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APBLDGINSPKEY, 
                    src.APBLDGKEY, 
                    src.APNO, 
                    src.APPLICATIONTYPE, 
                    src.CONTRACTOR, 
                    src.DATALAKE_DELETED, 
                    src.DP, 
                    src.ENGINEERFULLNAME, 
                    src.FLAT, 
                    src.HOUSENUMBER, 
                    src.LEGALOWNEREMAIL, 
                    src.LEGALOWNERFULLNAME, 
                    src.LEGALOWNERMOBILE, 
                    src.LOT, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NONDOMESTICCONSTAGINGKEY, 
                    src.ONSITEEMAIL, 
                    src.ONSITEFULLNAME, 
                    src.ONSITEMOBILE, 
                    src.POSTALCODE, 
                    src.REQUESTTYPE, 
                    src.STREETNAME, 
                    src.STREETTYPE, 
                    src.SUBURB, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLINTERFACE_NONDOMESTICCONSTAGING as target using (
                SELECT * FROM (SELECT 
            strm.ACTIONTYPE, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APBLDGINSPKEY, 
            strm.APBLDGKEY, 
            strm.APNO, 
            strm.APPLICATIONTYPE, 
            strm.CONTRACTOR, 
            strm.DATALAKE_DELETED, 
            strm.DP, 
            strm.ENGINEERFULLNAME, 
            strm.FLAT, 
            strm.HOUSENUMBER, 
            strm.LEGALOWNEREMAIL, 
            strm.LEGALOWNERFULLNAME, 
            strm.LEGALOWNERMOBILE, 
            strm.LOT, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NONDOMESTICCONSTAGINGKEY, 
            strm.ONSITEEMAIL, 
            strm.ONSITEFULLNAME, 
            strm.ONSITEMOBILE, 
            strm.POSTALCODE, 
            strm.REQUESTTYPE, 
            strm.STREETNAME, 
            strm.STREETTYPE, 
            strm.SUBURB, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.NONDOMESTICCONSTAGINGKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLINTERFACE_NONDOMESTICCONSTAGING as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.NONDOMESTICCONSTAGINGKEY=src.NONDOMESTICCONSTAGINGKEY) OR (target.NONDOMESTICCONSTAGINGKEY IS NULL AND src.NONDOMESTICCONSTAGINGKEY IS NULL)) 
                when matched then update set
                    target.ACTIONTYPE=src.ACTIONTYPE, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APBLDGINSPKEY=src.APBLDGINSPKEY, 
                    target.APBLDGKEY=src.APBLDGKEY, 
                    target.APNO=src.APNO, 
                    target.APPLICATIONTYPE=src.APPLICATIONTYPE, 
                    target.CONTRACTOR=src.CONTRACTOR, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DP=src.DP, 
                    target.ENGINEERFULLNAME=src.ENGINEERFULLNAME, 
                    target.FLAT=src.FLAT, 
                    target.HOUSENUMBER=src.HOUSENUMBER, 
                    target.LEGALOWNEREMAIL=src.LEGALOWNEREMAIL, 
                    target.LEGALOWNERFULLNAME=src.LEGALOWNERFULLNAME, 
                    target.LEGALOWNERMOBILE=src.LEGALOWNERMOBILE, 
                    target.LOT=src.LOT, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NONDOMESTICCONSTAGINGKEY=src.NONDOMESTICCONSTAGINGKEY, 
                    target.ONSITEEMAIL=src.ONSITEEMAIL, 
                    target.ONSITEFULLNAME=src.ONSITEFULLNAME, 
                    target.ONSITEMOBILE=src.ONSITEMOBILE, 
                    target.POSTALCODE=src.POSTALCODE, 
                    target.REQUESTTYPE=src.REQUESTTYPE, 
                    target.STREETNAME=src.STREETNAME, 
                    target.STREETTYPE=src.STREETTYPE, 
                    target.SUBURB=src.SUBURB, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACTIONTYPE, ADDBY, ADDDTTM, APBLDGINSPKEY, APBLDGKEY, APNO, APPLICATIONTYPE, CONTRACTOR, DATALAKE_DELETED, DP, ENGINEERFULLNAME, FLAT, HOUSENUMBER, LEGALOWNEREMAIL, LEGALOWNERFULLNAME, LEGALOWNERMOBILE, LOT, MODBY, MODDTTM, NONDOMESTICCONSTAGINGKEY, ONSITEEMAIL, ONSITEFULLNAME, ONSITEMOBILE, POSTALCODE, REQUESTTYPE, STREETNAME, STREETTYPE, SUBURB, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACTIONTYPE, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APBLDGINSPKEY, 
                    src.APBLDGKEY, 
                    src.APNO, 
                    src.APPLICATIONTYPE, 
                    src.CONTRACTOR, 
                    src.DATALAKE_DELETED, 
                    src.DP, 
                    src.ENGINEERFULLNAME, 
                    src.FLAT, 
                    src.HOUSENUMBER, 
                    src.LEGALOWNEREMAIL, 
                    src.LEGALOWNERFULLNAME, 
                    src.LEGALOWNERMOBILE, 
                    src.LOT, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NONDOMESTICCONSTAGINGKEY, 
                    src.ONSITEEMAIL, 
                    src.ONSITEFULLNAME, 
                    src.ONSITEMOBILE, 
                    src.POSTALCODE, 
                    src.REQUESTTYPE, 
                    src.STREETNAME, 
                    src.STREETTYPE, 
                    src.SUBURB, 
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
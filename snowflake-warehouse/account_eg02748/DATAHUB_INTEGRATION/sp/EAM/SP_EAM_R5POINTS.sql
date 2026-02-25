create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5POINTS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5POINTS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5POINTS as target using (SELECT * FROM (SELECT 
            strm.POI_CREATED, 
            strm.POI_DESC, 
            strm.POI_LASTSAVED, 
            strm.POI_OBJECT, 
            strm.POI_OBJECT_ORG, 
            strm.POI_OBRTYPE, 
            strm.POI_OBTYPE, 
            strm.POI_POINT, 
            strm.POI_POINTTYPE, 
            strm.POI_UDFCHAR01, 
            strm.POI_UDFCHAR02, 
            strm.POI_UDFCHAR03, 
            strm.POI_UDFCHAR04, 
            strm.POI_UDFCHAR05, 
            strm.POI_UDFCHAR06, 
            strm.POI_UDFCHAR07, 
            strm.POI_UDFCHAR08, 
            strm.POI_UDFCHAR09, 
            strm.POI_UDFCHAR10, 
            strm.POI_UDFCHAR11, 
            strm.POI_UDFCHAR12, 
            strm.POI_UDFCHAR13, 
            strm.POI_UDFCHAR14, 
            strm.POI_UDFCHAR15, 
            strm.POI_UDFCHAR16, 
            strm.POI_UDFCHAR17, 
            strm.POI_UDFCHAR18, 
            strm.POI_UDFCHAR19, 
            strm.POI_UDFCHAR20, 
            strm.POI_UDFCHAR21, 
            strm.POI_UDFCHAR22, 
            strm.POI_UDFCHAR23, 
            strm.POI_UDFCHAR24, 
            strm.POI_UDFCHAR25, 
            strm.POI_UDFCHAR26, 
            strm.POI_UDFCHAR27, 
            strm.POI_UDFCHAR28, 
            strm.POI_UDFCHAR29, 
            strm.POI_UDFCHAR30, 
            strm.POI_UDFCHKBOX01, 
            strm.POI_UDFCHKBOX02, 
            strm.POI_UDFCHKBOX03, 
            strm.POI_UDFCHKBOX04, 
            strm.POI_UDFCHKBOX05, 
            strm.POI_UDFDATE01, 
            strm.POI_UDFDATE02, 
            strm.POI_UDFDATE03, 
            strm.POI_UDFDATE04, 
            strm.POI_UDFDATE05, 
            strm.POI_UDFNUM01, 
            strm.POI_UDFNUM02, 
            strm.POI_UDFNUM03, 
            strm.POI_UDFNUM04, 
            strm.POI_UDFNUM05, 
            strm.POI_UPDATECOUNT, 
            strm.POI_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.POI_OBJECT,
            strm.POI_OBJECT_ORG,
            strm.POI_OBTYPE,
            strm.POI_POINT,
            strm.POI_POINTTYPE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5POINTS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.POI_OBJECT=src.POI_OBJECT) OR (target.POI_OBJECT IS NULL AND src.POI_OBJECT IS NULL)) AND
            ((target.POI_OBJECT_ORG=src.POI_OBJECT_ORG) OR (target.POI_OBJECT_ORG IS NULL AND src.POI_OBJECT_ORG IS NULL)) AND
            ((target.POI_OBTYPE=src.POI_OBTYPE) OR (target.POI_OBTYPE IS NULL AND src.POI_OBTYPE IS NULL)) AND
            ((target.POI_POINT=src.POI_POINT) OR (target.POI_POINT IS NULL AND src.POI_POINT IS NULL)) AND
            ((target.POI_POINTTYPE=src.POI_POINTTYPE) OR (target.POI_POINTTYPE IS NULL AND src.POI_POINTTYPE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.POI_CREATED=src.POI_CREATED, 
                    target.POI_DESC=src.POI_DESC, 
                    target.POI_LASTSAVED=src.POI_LASTSAVED, 
                    target.POI_OBJECT=src.POI_OBJECT, 
                    target.POI_OBJECT_ORG=src.POI_OBJECT_ORG, 
                    target.POI_OBRTYPE=src.POI_OBRTYPE, 
                    target.POI_OBTYPE=src.POI_OBTYPE, 
                    target.POI_POINT=src.POI_POINT, 
                    target.POI_POINTTYPE=src.POI_POINTTYPE, 
                    target.POI_UDFCHAR01=src.POI_UDFCHAR01, 
                    target.POI_UDFCHAR02=src.POI_UDFCHAR02, 
                    target.POI_UDFCHAR03=src.POI_UDFCHAR03, 
                    target.POI_UDFCHAR04=src.POI_UDFCHAR04, 
                    target.POI_UDFCHAR05=src.POI_UDFCHAR05, 
                    target.POI_UDFCHAR06=src.POI_UDFCHAR06, 
                    target.POI_UDFCHAR07=src.POI_UDFCHAR07, 
                    target.POI_UDFCHAR08=src.POI_UDFCHAR08, 
                    target.POI_UDFCHAR09=src.POI_UDFCHAR09, 
                    target.POI_UDFCHAR10=src.POI_UDFCHAR10, 
                    target.POI_UDFCHAR11=src.POI_UDFCHAR11, 
                    target.POI_UDFCHAR12=src.POI_UDFCHAR12, 
                    target.POI_UDFCHAR13=src.POI_UDFCHAR13, 
                    target.POI_UDFCHAR14=src.POI_UDFCHAR14, 
                    target.POI_UDFCHAR15=src.POI_UDFCHAR15, 
                    target.POI_UDFCHAR16=src.POI_UDFCHAR16, 
                    target.POI_UDFCHAR17=src.POI_UDFCHAR17, 
                    target.POI_UDFCHAR18=src.POI_UDFCHAR18, 
                    target.POI_UDFCHAR19=src.POI_UDFCHAR19, 
                    target.POI_UDFCHAR20=src.POI_UDFCHAR20, 
                    target.POI_UDFCHAR21=src.POI_UDFCHAR21, 
                    target.POI_UDFCHAR22=src.POI_UDFCHAR22, 
                    target.POI_UDFCHAR23=src.POI_UDFCHAR23, 
                    target.POI_UDFCHAR24=src.POI_UDFCHAR24, 
                    target.POI_UDFCHAR25=src.POI_UDFCHAR25, 
                    target.POI_UDFCHAR26=src.POI_UDFCHAR26, 
                    target.POI_UDFCHAR27=src.POI_UDFCHAR27, 
                    target.POI_UDFCHAR28=src.POI_UDFCHAR28, 
                    target.POI_UDFCHAR29=src.POI_UDFCHAR29, 
                    target.POI_UDFCHAR30=src.POI_UDFCHAR30, 
                    target.POI_UDFCHKBOX01=src.POI_UDFCHKBOX01, 
                    target.POI_UDFCHKBOX02=src.POI_UDFCHKBOX02, 
                    target.POI_UDFCHKBOX03=src.POI_UDFCHKBOX03, 
                    target.POI_UDFCHKBOX04=src.POI_UDFCHKBOX04, 
                    target.POI_UDFCHKBOX05=src.POI_UDFCHKBOX05, 
                    target.POI_UDFDATE01=src.POI_UDFDATE01, 
                    target.POI_UDFDATE02=src.POI_UDFDATE02, 
                    target.POI_UDFDATE03=src.POI_UDFDATE03, 
                    target.POI_UDFDATE04=src.POI_UDFDATE04, 
                    target.POI_UDFDATE05=src.POI_UDFDATE05, 
                    target.POI_UDFNUM01=src.POI_UDFNUM01, 
                    target.POI_UDFNUM02=src.POI_UDFNUM02, 
                    target.POI_UDFNUM03=src.POI_UDFNUM03, 
                    target.POI_UDFNUM04=src.POI_UDFNUM04, 
                    target.POI_UDFNUM05=src.POI_UDFNUM05, 
                    target.POI_UPDATECOUNT=src.POI_UPDATECOUNT, 
                    target.POI_UPDATED=src.POI_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    POI_CREATED, 
                    POI_DESC, 
                    POI_LASTSAVED, 
                    POI_OBJECT, 
                    POI_OBJECT_ORG, 
                    POI_OBRTYPE, 
                    POI_OBTYPE, 
                    POI_POINT, 
                    POI_POINTTYPE, 
                    POI_UDFCHAR01, 
                    POI_UDFCHAR02, 
                    POI_UDFCHAR03, 
                    POI_UDFCHAR04, 
                    POI_UDFCHAR05, 
                    POI_UDFCHAR06, 
                    POI_UDFCHAR07, 
                    POI_UDFCHAR08, 
                    POI_UDFCHAR09, 
                    POI_UDFCHAR10, 
                    POI_UDFCHAR11, 
                    POI_UDFCHAR12, 
                    POI_UDFCHAR13, 
                    POI_UDFCHAR14, 
                    POI_UDFCHAR15, 
                    POI_UDFCHAR16, 
                    POI_UDFCHAR17, 
                    POI_UDFCHAR18, 
                    POI_UDFCHAR19, 
                    POI_UDFCHAR20, 
                    POI_UDFCHAR21, 
                    POI_UDFCHAR22, 
                    POI_UDFCHAR23, 
                    POI_UDFCHAR24, 
                    POI_UDFCHAR25, 
                    POI_UDFCHAR26, 
                    POI_UDFCHAR27, 
                    POI_UDFCHAR28, 
                    POI_UDFCHAR29, 
                    POI_UDFCHAR30, 
                    POI_UDFCHKBOX01, 
                    POI_UDFCHKBOX02, 
                    POI_UDFCHKBOX03, 
                    POI_UDFCHKBOX04, 
                    POI_UDFCHKBOX05, 
                    POI_UDFDATE01, 
                    POI_UDFDATE02, 
                    POI_UDFDATE03, 
                    POI_UDFDATE04, 
                    POI_UDFDATE05, 
                    POI_UDFNUM01, 
                    POI_UDFNUM02, 
                    POI_UDFNUM03, 
                    POI_UDFNUM04, 
                    POI_UDFNUM05, 
                    POI_UPDATECOUNT, 
                    POI_UPDATED, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.POI_CREATED, 
                    src.POI_DESC, 
                    src.POI_LASTSAVED, 
                    src.POI_OBJECT, 
                    src.POI_OBJECT_ORG, 
                    src.POI_OBRTYPE, 
                    src.POI_OBTYPE, 
                    src.POI_POINT, 
                    src.POI_POINTTYPE, 
                    src.POI_UDFCHAR01, 
                    src.POI_UDFCHAR02, 
                    src.POI_UDFCHAR03, 
                    src.POI_UDFCHAR04, 
                    src.POI_UDFCHAR05, 
                    src.POI_UDFCHAR06, 
                    src.POI_UDFCHAR07, 
                    src.POI_UDFCHAR08, 
                    src.POI_UDFCHAR09, 
                    src.POI_UDFCHAR10, 
                    src.POI_UDFCHAR11, 
                    src.POI_UDFCHAR12, 
                    src.POI_UDFCHAR13, 
                    src.POI_UDFCHAR14, 
                    src.POI_UDFCHAR15, 
                    src.POI_UDFCHAR16, 
                    src.POI_UDFCHAR17, 
                    src.POI_UDFCHAR18, 
                    src.POI_UDFCHAR19, 
                    src.POI_UDFCHAR20, 
                    src.POI_UDFCHAR21, 
                    src.POI_UDFCHAR22, 
                    src.POI_UDFCHAR23, 
                    src.POI_UDFCHAR24, 
                    src.POI_UDFCHAR25, 
                    src.POI_UDFCHAR26, 
                    src.POI_UDFCHAR27, 
                    src.POI_UDFCHAR28, 
                    src.POI_UDFCHAR29, 
                    src.POI_UDFCHAR30, 
                    src.POI_UDFCHKBOX01, 
                    src.POI_UDFCHKBOX02, 
                    src.POI_UDFCHKBOX03, 
                    src.POI_UDFCHKBOX04, 
                    src.POI_UDFCHKBOX05, 
                    src.POI_UDFDATE01, 
                    src.POI_UDFDATE02, 
                    src.POI_UDFDATE03, 
                    src.POI_UDFDATE04, 
                    src.POI_UDFDATE05, 
                    src.POI_UDFNUM01, 
                    src.POI_UDFNUM02, 
                    src.POI_UDFNUM03, 
                    src.POI_UDFNUM04, 
                    src.POI_UDFNUM05, 
                    src.POI_UPDATECOUNT, 
                    src.POI_UPDATED, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5POINTS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.POI_CREATED, 
            strm.POI_DESC, 
            strm.POI_LASTSAVED, 
            strm.POI_OBJECT, 
            strm.POI_OBJECT_ORG, 
            strm.POI_OBRTYPE, 
            strm.POI_OBTYPE, 
            strm.POI_POINT, 
            strm.POI_POINTTYPE, 
            strm.POI_UDFCHAR01, 
            strm.POI_UDFCHAR02, 
            strm.POI_UDFCHAR03, 
            strm.POI_UDFCHAR04, 
            strm.POI_UDFCHAR05, 
            strm.POI_UDFCHAR06, 
            strm.POI_UDFCHAR07, 
            strm.POI_UDFCHAR08, 
            strm.POI_UDFCHAR09, 
            strm.POI_UDFCHAR10, 
            strm.POI_UDFCHAR11, 
            strm.POI_UDFCHAR12, 
            strm.POI_UDFCHAR13, 
            strm.POI_UDFCHAR14, 
            strm.POI_UDFCHAR15, 
            strm.POI_UDFCHAR16, 
            strm.POI_UDFCHAR17, 
            strm.POI_UDFCHAR18, 
            strm.POI_UDFCHAR19, 
            strm.POI_UDFCHAR20, 
            strm.POI_UDFCHAR21, 
            strm.POI_UDFCHAR22, 
            strm.POI_UDFCHAR23, 
            strm.POI_UDFCHAR24, 
            strm.POI_UDFCHAR25, 
            strm.POI_UDFCHAR26, 
            strm.POI_UDFCHAR27, 
            strm.POI_UDFCHAR28, 
            strm.POI_UDFCHAR29, 
            strm.POI_UDFCHAR30, 
            strm.POI_UDFCHKBOX01, 
            strm.POI_UDFCHKBOX02, 
            strm.POI_UDFCHKBOX03, 
            strm.POI_UDFCHKBOX04, 
            strm.POI_UDFCHKBOX05, 
            strm.POI_UDFDATE01, 
            strm.POI_UDFDATE02, 
            strm.POI_UDFDATE03, 
            strm.POI_UDFDATE04, 
            strm.POI_UDFDATE05, 
            strm.POI_UDFNUM01, 
            strm.POI_UDFNUM02, 
            strm.POI_UDFNUM03, 
            strm.POI_UDFNUM04, 
            strm.POI_UDFNUM05, 
            strm.POI_UPDATECOUNT, 
            strm.POI_UPDATED, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.POI_OBJECT,
            strm.POI_OBJECT_ORG,
            strm.POI_OBTYPE,
            strm.POI_POINT,
            strm.POI_POINTTYPE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5POINTS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.POI_OBJECT=src.POI_OBJECT) OR (target.POI_OBJECT IS NULL AND src.POI_OBJECT IS NULL)) AND
            ((target.POI_OBJECT_ORG=src.POI_OBJECT_ORG) OR (target.POI_OBJECT_ORG IS NULL AND src.POI_OBJECT_ORG IS NULL)) AND
            ((target.POI_OBTYPE=src.POI_OBTYPE) OR (target.POI_OBTYPE IS NULL AND src.POI_OBTYPE IS NULL)) AND
            ((target.POI_POINT=src.POI_POINT) OR (target.POI_POINT IS NULL AND src.POI_POINT IS NULL)) AND
            ((target.POI_POINTTYPE=src.POI_POINTTYPE) OR (target.POI_POINTTYPE IS NULL AND src.POI_POINTTYPE IS NULL)) 
                when matched then update set
                    target.POI_CREATED=src.POI_CREATED, 
                    target.POI_DESC=src.POI_DESC, 
                    target.POI_LASTSAVED=src.POI_LASTSAVED, 
                    target.POI_OBJECT=src.POI_OBJECT, 
                    target.POI_OBJECT_ORG=src.POI_OBJECT_ORG, 
                    target.POI_OBRTYPE=src.POI_OBRTYPE, 
                    target.POI_OBTYPE=src.POI_OBTYPE, 
                    target.POI_POINT=src.POI_POINT, 
                    target.POI_POINTTYPE=src.POI_POINTTYPE, 
                    target.POI_UDFCHAR01=src.POI_UDFCHAR01, 
                    target.POI_UDFCHAR02=src.POI_UDFCHAR02, 
                    target.POI_UDFCHAR03=src.POI_UDFCHAR03, 
                    target.POI_UDFCHAR04=src.POI_UDFCHAR04, 
                    target.POI_UDFCHAR05=src.POI_UDFCHAR05, 
                    target.POI_UDFCHAR06=src.POI_UDFCHAR06, 
                    target.POI_UDFCHAR07=src.POI_UDFCHAR07, 
                    target.POI_UDFCHAR08=src.POI_UDFCHAR08, 
                    target.POI_UDFCHAR09=src.POI_UDFCHAR09, 
                    target.POI_UDFCHAR10=src.POI_UDFCHAR10, 
                    target.POI_UDFCHAR11=src.POI_UDFCHAR11, 
                    target.POI_UDFCHAR12=src.POI_UDFCHAR12, 
                    target.POI_UDFCHAR13=src.POI_UDFCHAR13, 
                    target.POI_UDFCHAR14=src.POI_UDFCHAR14, 
                    target.POI_UDFCHAR15=src.POI_UDFCHAR15, 
                    target.POI_UDFCHAR16=src.POI_UDFCHAR16, 
                    target.POI_UDFCHAR17=src.POI_UDFCHAR17, 
                    target.POI_UDFCHAR18=src.POI_UDFCHAR18, 
                    target.POI_UDFCHAR19=src.POI_UDFCHAR19, 
                    target.POI_UDFCHAR20=src.POI_UDFCHAR20, 
                    target.POI_UDFCHAR21=src.POI_UDFCHAR21, 
                    target.POI_UDFCHAR22=src.POI_UDFCHAR22, 
                    target.POI_UDFCHAR23=src.POI_UDFCHAR23, 
                    target.POI_UDFCHAR24=src.POI_UDFCHAR24, 
                    target.POI_UDFCHAR25=src.POI_UDFCHAR25, 
                    target.POI_UDFCHAR26=src.POI_UDFCHAR26, 
                    target.POI_UDFCHAR27=src.POI_UDFCHAR27, 
                    target.POI_UDFCHAR28=src.POI_UDFCHAR28, 
                    target.POI_UDFCHAR29=src.POI_UDFCHAR29, 
                    target.POI_UDFCHAR30=src.POI_UDFCHAR30, 
                    target.POI_UDFCHKBOX01=src.POI_UDFCHKBOX01, 
                    target.POI_UDFCHKBOX02=src.POI_UDFCHKBOX02, 
                    target.POI_UDFCHKBOX03=src.POI_UDFCHKBOX03, 
                    target.POI_UDFCHKBOX04=src.POI_UDFCHKBOX04, 
                    target.POI_UDFCHKBOX05=src.POI_UDFCHKBOX05, 
                    target.POI_UDFDATE01=src.POI_UDFDATE01, 
                    target.POI_UDFDATE02=src.POI_UDFDATE02, 
                    target.POI_UDFDATE03=src.POI_UDFDATE03, 
                    target.POI_UDFDATE04=src.POI_UDFDATE04, 
                    target.POI_UDFDATE05=src.POI_UDFDATE05, 
                    target.POI_UDFNUM01=src.POI_UDFNUM01, 
                    target.POI_UDFNUM02=src.POI_UDFNUM02, 
                    target.POI_UDFNUM03=src.POI_UDFNUM03, 
                    target.POI_UDFNUM04=src.POI_UDFNUM04, 
                    target.POI_UDFNUM05=src.POI_UDFNUM05, 
                    target.POI_UPDATECOUNT=src.POI_UPDATECOUNT, 
                    target.POI_UPDATED=src.POI_UPDATED, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( POI_CREATED, POI_DESC, POI_LASTSAVED, POI_OBJECT, POI_OBJECT_ORG, POI_OBRTYPE, POI_OBTYPE, POI_POINT, POI_POINTTYPE, POI_UDFCHAR01, POI_UDFCHAR02, POI_UDFCHAR03, POI_UDFCHAR04, POI_UDFCHAR05, POI_UDFCHAR06, POI_UDFCHAR07, POI_UDFCHAR08, POI_UDFCHAR09, POI_UDFCHAR10, POI_UDFCHAR11, POI_UDFCHAR12, POI_UDFCHAR13, POI_UDFCHAR14, POI_UDFCHAR15, POI_UDFCHAR16, POI_UDFCHAR17, POI_UDFCHAR18, POI_UDFCHAR19, POI_UDFCHAR20, POI_UDFCHAR21, POI_UDFCHAR22, POI_UDFCHAR23, POI_UDFCHAR24, POI_UDFCHAR25, POI_UDFCHAR26, POI_UDFCHAR27, POI_UDFCHAR28, POI_UDFCHAR29, POI_UDFCHAR30, POI_UDFCHKBOX01, POI_UDFCHKBOX02, POI_UDFCHKBOX03, POI_UDFCHKBOX04, POI_UDFCHKBOX05, POI_UDFDATE01, POI_UDFDATE02, POI_UDFDATE03, POI_UDFDATE04, POI_UDFDATE05, POI_UDFNUM01, POI_UDFNUM02, POI_UDFNUM03, POI_UDFNUM04, POI_UDFNUM05, POI_UPDATECOUNT, POI_UPDATED, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.POI_CREATED, 
                    src.POI_DESC, 
                    src.POI_LASTSAVED, 
                    src.POI_OBJECT, 
                    src.POI_OBJECT_ORG, 
                    src.POI_OBRTYPE, 
                    src.POI_OBTYPE, 
                    src.POI_POINT, 
                    src.POI_POINTTYPE, 
                    src.POI_UDFCHAR01, 
                    src.POI_UDFCHAR02, 
                    src.POI_UDFCHAR03, 
                    src.POI_UDFCHAR04, 
                    src.POI_UDFCHAR05, 
                    src.POI_UDFCHAR06, 
                    src.POI_UDFCHAR07, 
                    src.POI_UDFCHAR08, 
                    src.POI_UDFCHAR09, 
                    src.POI_UDFCHAR10, 
                    src.POI_UDFCHAR11, 
                    src.POI_UDFCHAR12, 
                    src.POI_UDFCHAR13, 
                    src.POI_UDFCHAR14, 
                    src.POI_UDFCHAR15, 
                    src.POI_UDFCHAR16, 
                    src.POI_UDFCHAR17, 
                    src.POI_UDFCHAR18, 
                    src.POI_UDFCHAR19, 
                    src.POI_UDFCHAR20, 
                    src.POI_UDFCHAR21, 
                    src.POI_UDFCHAR22, 
                    src.POI_UDFCHAR23, 
                    src.POI_UDFCHAR24, 
                    src.POI_UDFCHAR25, 
                    src.POI_UDFCHAR26, 
                    src.POI_UDFCHAR27, 
                    src.POI_UDFCHAR28, 
                    src.POI_UDFCHAR29, 
                    src.POI_UDFCHAR30, 
                    src.POI_UDFCHKBOX01, 
                    src.POI_UDFCHKBOX02, 
                    src.POI_UDFCHKBOX03, 
                    src.POI_UDFCHKBOX04, 
                    src.POI_UDFCHKBOX05, 
                    src.POI_UDFDATE01, 
                    src.POI_UDFDATE02, 
                    src.POI_UDFDATE03, 
                    src.POI_UDFDATE04, 
                    src.POI_UDFDATE05, 
                    src.POI_UDFNUM01, 
                    src.POI_UDFNUM02, 
                    src.POI_UDFNUM03, 
                    src.POI_UDFNUM04, 
                    src.POI_UDFNUM05, 
                    src.POI_UPDATECOUNT, 
                    src.POI_UPDATED, 
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
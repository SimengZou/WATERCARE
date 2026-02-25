
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ADDETAILS
                   as                       
                    SELECT
                        t.ADD_ENTITY,
                        t.ADD_RENTITY,
                        t.ADD_TYPE,
                        t.ADD_RTYPE,
                        t.ADD_CODE,
                        t.ADD_LANG,
                        t.ADD_LINE,
                        t.ADD_PRINT,
                        t.ADD_TEXT,
                        t.ADD_CREATED,
                        t.ADD_USER,
                        t.ADD_UPDATED,
                        t.ADD_UPDUSER,
                        t.ADD_UPDATECOUNT,
                        t.ADD_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ADDETAILS as  t
					 union
					        SELECT
                        th.ADD_ENTITY,
                        th.ADD_RENTITY,
                        th.ADD_TYPE,
                        th.ADD_RTYPE,
                        th.ADD_CODE,
                        th.ADD_LANG,
                        th.ADD_LINE,
                        th.ADD_PRINT,
                        th.ADD_TEXT,
                        th.ADD_CREATED,
                        th.ADD_USER,
                        th.ADD_UPDATED,
                        th.ADD_UPDUSER,
                        th.ADD_UPDATECOUNT,
                        th.ADD_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ADDETAILS as  th ;
                     
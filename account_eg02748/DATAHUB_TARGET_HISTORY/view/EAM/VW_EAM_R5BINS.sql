
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5BINS
                   as                       
                    SELECT
                        t.BIN_STORE,
                        t.BIN_CODE,
                        t.BIN_DESC,
                        t.BIN_CREATED,
                        t.BIN_CREATEDBY,
                        t.BIN_UPDATED,
                        t.BIN_UPDATEDBY,
                        t.BIN_NOTUSED,
                        t.BIN_UPDATECOUNT,
                        t.BIN_FORSTOCK,
                        t.BIN_FORHELDITEMS,
                        t.BIN_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5BINS as  t
					 union
					        SELECT
                        th.BIN_STORE,
                        th.BIN_CODE,
                        th.BIN_DESC,
                        th.BIN_CREATED,
                        th.BIN_CREATEDBY,
                        th.BIN_UPDATED,
                        th.BIN_UPDATEDBY,
                        th.BIN_NOTUSED,
                        th.BIN_UPDATECOUNT,
                        th.BIN_FORSTOCK,
                        th.BIN_FORHELDITEMS,
                        th.BIN_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5BINS as  th ;
                     
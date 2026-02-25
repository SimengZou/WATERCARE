
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5SPOTCODES
                   as                       
                    SELECT
                        t.DESCRIPTION,
                        t.SPC_CODE,
                        t.SPC_DESC,
                        t.SPC_TYPE,
                        t.SPC_NOTUSED,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.LASTSAVED,
                        t.UPDATECOUNT,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5SPOTCODES as  t
					 union
					        SELECT
                        th.DESCRIPTION,
                        th.SPC_CODE,
                        th.SPC_DESC,
                        th.SPC_TYPE,
                        th.SPC_NOTUSED,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.LASTSAVED,
                        th.UPDATECOUNT,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5SPOTCODES as  th ;
                     
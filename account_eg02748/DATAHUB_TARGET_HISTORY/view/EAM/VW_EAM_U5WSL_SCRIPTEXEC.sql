
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5WSL_SCRIPTEXEC
                   as                       
                    SELECT
                        t.SNO,
                        t.DESCRIPTION,
                        t.SCRIPT,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.UPDATECOUNT,
                        t.LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5WSL_SCRIPTEXEC as  t
					 union
					        SELECT
                        th.SNO,
                        th.DESCRIPTION,
                        th.SCRIPT,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.UPDATECOUNT,
                        th.LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5WSL_SCRIPTEXEC as  th ;
                     
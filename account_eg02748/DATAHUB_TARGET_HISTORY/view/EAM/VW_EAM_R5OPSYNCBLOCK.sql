
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5OPSYNCBLOCK
                   as                       
                    SELECT
                        t.OPB_BLOCKID,
                        t.OPB_SYNCTABLE,
                        t.OPB_STARTVAL,
                        t.OPB_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5OPSYNCBLOCK as  t
					 union
					        SELECT
                        th.OPB_BLOCKID,
                        th.OPB_SYNCTABLE,
                        th.OPB_STARTVAL,
                        th.OPB_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5OPSYNCBLOCK as  th ;
                     
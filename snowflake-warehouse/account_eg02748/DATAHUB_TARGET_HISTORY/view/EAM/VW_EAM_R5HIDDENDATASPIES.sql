
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5HIDDENDATASPIES
                   as                       
                    SELECT
                        t.HDS_GROUP,
                        t.HDS_DATASPYID,
                        t.HDS_UPDATECOUNT,
                        t.HDS_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5HIDDENDATASPIES as  t
					 union
					        SELECT
                        th.HDS_GROUP,
                        th.HDS_DATASPYID,
                        th.HDS_UPDATECOUNT,
                        th.HDS_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5HIDDENDATASPIES as  th ;
                     
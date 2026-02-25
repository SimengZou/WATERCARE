
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5UNAVAILABLEGRIDFIELDS
                   as                       
                    SELECT
                        t.UGF_USERGROUP,
                        t.UGF_GRIDNAME,
                        t.UGF_FIELDID,
                        t.UGF_MEKEY,
                        t.UGF_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5UNAVAILABLEGRIDFIELDS as  t
					 union
					        SELECT
                        th.UGF_USERGROUP,
                        th.UGF_GRIDNAME,
                        th.UGF_FIELDID,
                        th.UGF_MEKEY,
                        th.UGF_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5UNAVAILABLEGRIDFIELDS as  th ;
                     

                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MAILURLMAPPING
                   as                       
                    SELECT
                        t.MUM_TABLE,
                        t.MUM_FUNCTION,
                        t.MUM_JSPFIELD,
                        t.MUM_TABLECOLUMN,
                        t.MUM_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MAILURLMAPPING as  t
					 union
					        SELECT
                        th.MUM_TABLE,
                        th.MUM_FUNCTION,
                        th.MUM_JSPFIELD,
                        th.MUM_TABLECOLUMN,
                        th.MUM_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MAILURLMAPPING as  th ;
                     
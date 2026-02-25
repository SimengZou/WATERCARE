
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5GLREFERENCESCTRL
                   as                       
                    SELECT
                        t.GRC_R5COLUMN,
                        t.GRC_DISPLAYNAME,
                        t.GRC_LENGTH,
                        t.GRC_DATATYPE,
                        t.GRC_DISPLAYSEQ,
                        t.GRC_UPDATECOUNT,
                        t.GRC_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5GLREFERENCESCTRL as  t
					 union
					        SELECT
                        th.GRC_R5COLUMN,
                        th.GRC_DISPLAYNAME,
                        th.GRC_LENGTH,
                        th.GRC_DATATYPE,
                        th.GRC_DISPLAYSEQ,
                        th.GRC_UPDATECOUNT,
                        th.GRC_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5GLREFERENCESCTRL as  th ;
                     
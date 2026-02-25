
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5JSPFIELDS
                   as                       
                    SELECT
                        t.JFD_JSPFILE,
                        t.JFD_FIELDS,
                        t.JFD_OTHERFIELDS,
                        t.JFD_JSINCLUDES,
                        t.JFD_UPDATECOUNT,
                        t.JFD_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5JSPFIELDS as  t
					 union
					        SELECT
                        th.JFD_JSPFILE,
                        th.JFD_FIELDS,
                        th.JFD_OTHERFIELDS,
                        th.JFD_JSINCLUDES,
                        th.JFD_UPDATECOUNT,
                        th.JFD_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5JSPFIELDS as  th ;
                     
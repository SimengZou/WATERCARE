
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5LABELFORMATS
                   as                       
                    SELECT
                        t.LBL_CODE,
                        t.LBL_FILENAME,
                        t.LBL_DESC,
                        t.LBL_PRINTERTYPE,
                        t.LBL_FIELDS,
                        t.LBL_CLASS,
                        t.LBL_CLASS_ORG,
                        t.LBL_UPDATECOUNT,
                        t.LBL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5LABELFORMATS as  t
					 union
					        SELECT
                        th.LBL_CODE,
                        th.LBL_FILENAME,
                        th.LBL_DESC,
                        th.LBL_PRINTERTYPE,
                        th.LBL_FIELDS,
                        th.LBL_CLASS,
                        th.LBL_CLASS_ORG,
                        th.LBL_UPDATECOUNT,
                        th.LBL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5LABELFORMATS as  th ;
                     
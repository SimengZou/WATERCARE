
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5IMPORTEXPORTGRPSTATUS
                   as                       
                    SELECT
                        t.IMG_SESSIONID,
                        t.IMG_GROUP,
                        t.IMG_STATUS,
                        t.IMG_PROCESSORDER,
                        t.IMG_START,
                        t.IMG_END,
                        t.IMG_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5IMPORTEXPORTGRPSTATUS as  t
					 union
					        SELECT
                        th.IMG_SESSIONID,
                        th.IMG_GROUP,
                        th.IMG_STATUS,
                        th.IMG_PROCESSORDER,
                        th.IMG_START,
                        th.IMG_END,
                        th.IMG_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5IMPORTEXPORTGRPSTATUS as  th ;
                     
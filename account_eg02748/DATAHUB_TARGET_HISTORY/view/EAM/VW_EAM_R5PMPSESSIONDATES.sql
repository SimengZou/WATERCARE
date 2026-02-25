
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PMPSESSIONDATES
                   as                       
                    SELECT
                        t.PPD_LINE,
                        t.PPD_PPSPK,
                        t.PPD_DUEDATE,
                        t.PPD_WORKORDER,
                        t.PPD_ISCALDATE,
                        t.PPD_ISPROJECTED,
                        t.PPD_UPDATECOUNT,
                        t.PPD_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PMPSESSIONDATES as  t
					 union
					        SELECT
                        th.PPD_LINE,
                        th.PPD_PPSPK,
                        th.PPD_DUEDATE,
                        th.PPD_WORKORDER,
                        th.PPD_ISCALDATE,
                        th.PPD_ISPROJECTED,
                        th.PPD_UPDATECOUNT,
                        th.PPD_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PMPSESSIONDATES as  th ;
                     
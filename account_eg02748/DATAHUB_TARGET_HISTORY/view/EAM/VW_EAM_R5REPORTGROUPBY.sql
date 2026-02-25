
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5REPORTGROUPBY
                   as                       
                    SELECT
                        t.RGP_FUNCTION,
                        t.RGP_LINE,
                        t.RGP_GROUPFIELDS,
                        t.RGP_DEFAULT,
                        t.RGP_UPDATECOUNT,
                        t.RGP_UPDATED,
                        t.RGP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5REPORTGROUPBY as  t
					 union
					        SELECT
                        th.RGP_FUNCTION,
                        th.RGP_LINE,
                        th.RGP_GROUPFIELDS,
                        th.RGP_DEFAULT,
                        th.RGP_UPDATECOUNT,
                        th.RGP_UPDATED,
                        th.RGP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5REPORTGROUPBY as  th ;
                     
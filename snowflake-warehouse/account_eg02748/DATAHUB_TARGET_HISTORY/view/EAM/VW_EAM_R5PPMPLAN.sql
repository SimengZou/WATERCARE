
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PPMPLAN
                   as                       
                    SELECT
                        t.PMP_CODE,
                        t.PMP_DESC,
                        t.PMP_ORG,
                        t.PMP_CLASS,
                        t.PMP_CLASS_ORG,
                        t.PMP_OBJECTCLASS,
                        t.PMP_OBJECTCLASS_ORG,
                        t.PMP_UPDATECOUNT,
                        t.PMP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PPMPLAN as  t
					 union
					        SELECT
                        th.PMP_CODE,
                        th.PMP_DESC,
                        th.PMP_ORG,
                        th.PMP_CLASS,
                        th.PMP_CLASS_ORG,
                        th.PMP_OBJECTCLASS,
                        th.PMP_OBJECTCLASS_ORG,
                        th.PMP_UPDATECOUNT,
                        th.PMP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PPMPLAN as  th ;
                     
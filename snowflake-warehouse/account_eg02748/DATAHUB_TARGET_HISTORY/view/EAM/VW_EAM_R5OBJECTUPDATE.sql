
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5OBJECTUPDATE
                   as                       
                    SELECT
                        t.OUP_PK,
                        t.OUP_OLDCODE,
                        t.OUP_ORG,
                        t.OUP_NEWCODE,
                        t.OUP_USER,
                        t.OUP_DATE,
                        t.OUP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5OBJECTUPDATE as  t
					 union
					        SELECT
                        th.OUP_PK,
                        th.OUP_OLDCODE,
                        th.OUP_ORG,
                        th.OUP_NEWCODE,
                        th.OUP_USER,
                        th.OUP_DATE,
                        th.OUP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5OBJECTUPDATE as  th ;
                     
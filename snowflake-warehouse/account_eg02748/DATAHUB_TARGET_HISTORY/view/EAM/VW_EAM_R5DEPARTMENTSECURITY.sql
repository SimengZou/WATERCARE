
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DEPARTMENTSECURITY
                   as                       
                    SELECT
                        t.DSE_USER,
                        t.DSE_MRC,
                        t.DSE_UPDATECOUNT,
                        t.DSE_READONLY,
                        t.DSE_UPDATED,
                        t.DSE_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DEPARTMENTSECURITY as  t
					 union
					        SELECT
                        th.DSE_USER,
                        th.DSE_MRC,
                        th.DSE_UPDATECOUNT,
                        th.DSE_READONLY,
                        th.DSE_UPDATED,
                        th.DSE_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DEPARTMENTSECURITY as  th ;
                     
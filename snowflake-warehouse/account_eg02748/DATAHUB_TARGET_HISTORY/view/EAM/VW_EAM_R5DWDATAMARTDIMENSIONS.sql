
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DWDATAMARTDIMENSIONS
                   as                       
                    SELECT
                        t.DMD_DMTTABLE,
                        t.DMD_DIMTABLE,
                        t.DMD_FACTJOINFIELD,
                        t.DMD_DIMBUSINESSKEYFIELD,
                        t.DMD_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DWDATAMARTDIMENSIONS as  t
					 union
					        SELECT
                        th.DMD_DMTTABLE,
                        th.DMD_DIMTABLE,
                        th.DMD_FACTJOINFIELD,
                        th.DMD_DIMBUSINESSKEYFIELD,
                        th.DMD_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DWDATAMARTDIMENSIONS as  th ;
                     
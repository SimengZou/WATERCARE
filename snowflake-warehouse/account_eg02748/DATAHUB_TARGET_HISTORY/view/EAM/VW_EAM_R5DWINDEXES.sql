
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DWINDEXES
                   as                       
                    SELECT
                        t.IDX_INDEXNAME,
                        t.IDX_TABLE,
                        t.IDX_FIELDS,
                        t.IDX_TABLESPACE,
                        t.IDX_INDEXTYPE,
                        t.IDX_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DWINDEXES as  t
					 union
					        SELECT
                        th.IDX_INDEXNAME,
                        th.IDX_TABLE,
                        th.IDX_FIELDS,
                        th.IDX_TABLESPACE,
                        th.IDX_INDEXTYPE,
                        th.IDX_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DWINDEXES as  th ;
                     
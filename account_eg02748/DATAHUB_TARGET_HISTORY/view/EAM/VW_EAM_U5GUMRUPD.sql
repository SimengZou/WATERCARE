
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5GUMRUPD
                   as                       
                    SELECT
                        t.MRU_TRANSID,
                        t.MRU_EVENT,
                        t.MRU_CONTRACTORCODE,
                        t.MRU_UPDATETYPE,
                        t.MRU_UPDATEDATE,
                        t.MRU_COMMENTS,
                        t.MRU_NUMBER,
                        t.MRU_REASON,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.LASTSAVED,
                        t.UPDATECOUNT,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5GUMRUPD as  t
					 union
					        SELECT
                        th.MRU_TRANSID,
                        th.MRU_EVENT,
                        th.MRU_CONTRACTORCODE,
                        th.MRU_UPDATETYPE,
                        th.MRU_UPDATEDATE,
                        th.MRU_COMMENTS,
                        th.MRU_NUMBER,
                        th.MRU_REASON,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.LASTSAVED,
                        th.UPDATECOUNT,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5GUMRUPD as  th ;
                     
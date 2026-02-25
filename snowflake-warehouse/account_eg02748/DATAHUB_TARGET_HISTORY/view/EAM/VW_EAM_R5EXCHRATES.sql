
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5EXCHRATES
                   as                       
                    SELECT
                        t.CRR_CURR,
                        t.CRR_EXCH,
                        t.CRR_START,
                        t.CRR_END,
                        t.CRR_ACTIVE,
                        t.CRR_SOURCESYSTEM,
                        t.CRR_SOURCECODE,
                        t.CRR_INTERFACE,
                        t.CRR_ORGCURR,
                        t.CRR_UPDATECOUNT,
                        t.CRR_CODE,
                        t.CRR_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5EXCHRATES as  t
					 union
					        SELECT
                        th.CRR_CURR,
                        th.CRR_EXCH,
                        th.CRR_START,
                        th.CRR_END,
                        th.CRR_ACTIVE,
                        th.CRR_SOURCESYSTEM,
                        th.CRR_SOURCECODE,
                        th.CRR_INTERFACE,
                        th.CRR_ORGCURR,
                        th.CRR_UPDATECOUNT,
                        th.CRR_CODE,
                        th.CRR_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5EXCHRATES as  th ;
                     
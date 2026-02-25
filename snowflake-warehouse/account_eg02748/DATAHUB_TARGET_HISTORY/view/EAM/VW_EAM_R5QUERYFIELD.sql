
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5QUERYFIELD
                   as                       
                    SELECT
                        t.DQF_DDSPYID,
                        t.DQF_FIELDID,
                        t.DQF_COLUMNWIDTH,
                        t.DQF_COLUMNORDER,
                        t.DQF_UPDATECOUNT,
                        t.DQF_VIEWTYPE,
                        t.DQF_UPDATED,
                        t.DQF_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5QUERYFIELD as  t
					 union
					        SELECT
                        th.DQF_DDSPYID,
                        th.DQF_FIELDID,
                        th.DQF_COLUMNWIDTH,
                        th.DQF_COLUMNORDER,
                        th.DQF_UPDATECOUNT,
                        th.DQF_VIEWTYPE,
                        th.DQF_UPDATED,
                        th.DQF_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5QUERYFIELD as  th ;
                     
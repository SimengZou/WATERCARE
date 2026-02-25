
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5GRIDPARAM
                   as                       
                    SELECT
                        t.GDP_GRIDID,
                        t.GDP_PARAM,
                        t.GDP_TAGNAME,
                        t.GDP_DATATYPE,
                        t.GDP_UPDATECOUNT,
                        t.GDP_UPDATED,
                        t.GDP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5GRIDPARAM as  t
					 union
					        SELECT
                        th.GDP_GRIDID,
                        th.GDP_PARAM,
                        th.GDP_TAGNAME,
                        th.GDP_DATATYPE,
                        th.GDP_UPDATECOUNT,
                        th.GDP_UPDATED,
                        th.GDP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5GRIDPARAM as  th ;
                     
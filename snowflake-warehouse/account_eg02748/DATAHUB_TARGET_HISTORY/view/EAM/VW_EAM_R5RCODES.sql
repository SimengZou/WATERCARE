
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5RCODES
                   as                       
                    SELECT
                        t.RCO_RENTITY,
                        t.RCO_RCODE,
                        t.RCO_DESC,
                        t.RCO_UPDATECOUNT,
                        t.RCO_TEXTCODE,
                        t.RCO_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5RCODES as  t
					 union
					        SELECT
                        th.RCO_RENTITY,
                        th.RCO_RCODE,
                        th.RCO_DESC,
                        th.RCO_UPDATECOUNT,
                        th.RCO_TEXTCODE,
                        th.RCO_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5RCODES as  th ;
                     
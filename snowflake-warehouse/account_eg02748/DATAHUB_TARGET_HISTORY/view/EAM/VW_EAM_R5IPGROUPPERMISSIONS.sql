
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5IPGROUPPERMISSIONS
                   as                       
                    SELECT
                        t.IPG_GROUP,
                        t.IPG_PERMISSION,
                        t.IPG_UPDATECOUNT,
                        t.IPG_ACTIVE,
                        t.IPG_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5IPGROUPPERMISSIONS as  t
					 union
					        SELECT
                        th.IPG_GROUP,
                        th.IPG_PERMISSION,
                        th.IPG_UPDATECOUNT,
                        th.IPG_ACTIVE,
                        th.IPG_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5IPGROUPPERMISSIONS as  th ;
                     
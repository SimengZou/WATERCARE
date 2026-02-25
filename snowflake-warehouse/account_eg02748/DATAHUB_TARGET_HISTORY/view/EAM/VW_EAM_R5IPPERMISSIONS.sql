
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5IPPERMISSIONS
                   as                       
                    SELECT
                        t.IPP_CODE,
                        t.IPP_MNEMONIC,
                        t.IPP_FUNCTION,
                        t.IPP_UPDATECOUNT,
                        t.IPP_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5IPPERMISSIONS as  t
					 union
					        SELECT
                        th.IPP_CODE,
                        th.IPP_MNEMONIC,
                        th.IPP_FUNCTION,
                        th.IPP_UPDATECOUNT,
                        th.IPP_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5IPPERMISSIONS as  th ;
                     
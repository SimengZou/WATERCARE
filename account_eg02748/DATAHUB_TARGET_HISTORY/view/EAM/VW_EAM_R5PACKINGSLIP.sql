
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PACKINGSLIP
                   as                       
                    SELECT
                        t.PSL_DOCK,
                        t.PSL_LINE,
                        t.PSL_ORDER,
                        t.PSL_ORDLINE,
                        t.PSL_ORDER_ORG,
                        t.PSL_PART,
                        t.PSL_PART_ORG,
                        t.PSL_MANLOT,
                        t.PSL_MANLOTEXP,
                        t.PSL_SUPPLIERREF,
                        t.PSL_DELUOM,
                        t.PSL_DELQTY,
                        t.PSL_RECVQTY,
                        t.PSL_MULTIPLY,
                        t.PSL_COMMENT,
                        t.PSL_UPDATECOUNT,
                        t.PSL_LASTSAVED,
                        t.PSL_BIN,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PACKINGSLIP as  t
					 union
					        SELECT
                        th.PSL_DOCK,
                        th.PSL_LINE,
                        th.PSL_ORDER,
                        th.PSL_ORDLINE,
                        th.PSL_ORDER_ORG,
                        th.PSL_PART,
                        th.PSL_PART_ORG,
                        th.PSL_MANLOT,
                        th.PSL_MANLOTEXP,
                        th.PSL_SUPPLIERREF,
                        th.PSL_DELUOM,
                        th.PSL_DELQTY,
                        th.PSL_RECVQTY,
                        th.PSL_MULTIPLY,
                        th.PSL_COMMENT,
                        th.PSL_UPDATECOUNT,
                        th.PSL_LASTSAVED,
                        th.PSL_BIN,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PACKINGSLIP as  th ;
                     
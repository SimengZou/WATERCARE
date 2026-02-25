
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5CONPARTS
                   as                       
                    SELECT
                        t.CPA_CONTRACT,
                        t.CPA_SUPPLIER,
                        t.CPA_PART,
                        t.CPA_MULTIPLY,
                        t.CPA_LEADTIME,
                        t.CPA_PRICE,
                        t.CPA_REF,
                        t.CPA_PURUOM,
                        t.CPA_SUPPLIER_ORG,
                        t.CPA_PART_ORG,
                        t.CPA_UPDATECOUNT,
                        t.CPA_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5CONPARTS as  t
					 union
					        SELECT
                        th.CPA_CONTRACT,
                        th.CPA_SUPPLIER,
                        th.CPA_PART,
                        th.CPA_MULTIPLY,
                        th.CPA_LEADTIME,
                        th.CPA_PRICE,
                        th.CPA_REF,
                        th.CPA_PURUOM,
                        th.CPA_SUPPLIER_ORG,
                        th.CPA_PART_ORG,
                        th.CPA_UPDATECOUNT,
                        th.CPA_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5CONPARTS as  th ;
                     
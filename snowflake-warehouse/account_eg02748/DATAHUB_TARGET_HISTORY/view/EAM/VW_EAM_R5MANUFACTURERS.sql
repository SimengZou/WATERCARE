
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MANUFACTURERS
                   as                       
                    SELECT
                        t.MFG_CODE,
                        t.MFG_DESC,
                        t.MFG_SUPPLIER,
                        t.MFG_CLASS,
                        t.MFG_SOURCESYSTEM,
                        t.MFG_SOURCECODE,
                        t.MFG_ORG,
                        t.MFG_CLASS_ORG,
                        t.MFG_SUPPLIER_ORG,
                        t.MFG_NOTUSED,
                        t.MFG_UPDATECOUNT,
                        t.MFG_UPDATED,
                        t.MFG_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MANUFACTURERS as  t
					 union
					        SELECT
                        th.MFG_CODE,
                        th.MFG_DESC,
                        th.MFG_SUPPLIER,
                        th.MFG_CLASS,
                        th.MFG_SOURCESYSTEM,
                        th.MFG_SOURCECODE,
                        th.MFG_ORG,
                        th.MFG_CLASS_ORG,
                        th.MFG_SUPPLIER_ORG,
                        th.MFG_NOTUSED,
                        th.MFG_UPDATECOUNT,
                        th.MFG_UPDATED,
                        th.MFG_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MANUFACTURERS as  th ;
                     
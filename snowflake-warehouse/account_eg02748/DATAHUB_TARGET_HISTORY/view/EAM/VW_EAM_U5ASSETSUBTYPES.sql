
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5ASSETSUBTYPES
                   as                       
                    SELECT
                        t.DESCRIPTION,
                        t.AST_ASSETTYPE,
                        t.AST_ASSETSUBTYPE,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.UPDATECOUNT,
                        t.LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5ASSETSUBTYPES as  t
					 union
					        SELECT
                        th.DESCRIPTION,
                        th.AST_ASSETTYPE,
                        th.AST_ASSETSUBTYPE,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.UPDATECOUNT,
                        th.LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5ASSETSUBTYPES as  th ;
                     
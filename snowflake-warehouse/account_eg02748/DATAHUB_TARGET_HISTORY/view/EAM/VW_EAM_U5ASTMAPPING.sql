
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5ASTMAPPING
                   as                       
                    SELECT
                        t.AUTOID,
                        t.AST_ID,
                        t.AST_CLASS,
                        t.AST_CLASSDESC,
                        t.AST_ATTRIBUTE,
                        t.AST_ATTRBDESC,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.LASTSAVED,
                        t.UPDATECOUNT,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5ASTMAPPING as  t
					 union
					        SELECT
                        th.AUTOID,
                        th.AST_ID,
                        th.AST_CLASS,
                        th.AST_CLASSDESC,
                        th.AST_ATTRIBUTE,
                        th.AST_ATTRBDESC,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.LASTSAVED,
                        th.UPDATECOUNT,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5ASTMAPPING as  th ;
                     
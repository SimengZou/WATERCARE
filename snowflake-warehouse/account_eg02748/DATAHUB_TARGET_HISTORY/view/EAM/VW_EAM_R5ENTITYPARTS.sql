
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ENTITYPARTS
                   as                       
                    SELECT
                        t.EPA_PART,
                        t.EPA_ENTITY,
                        t.EPA_RENTITY,
                        t.EPA_TYPE,
                        t.EPA_RTYPE,
                        t.EPA_CODE,
                        t.EPA_QTY,
                        t.EPA_UOM,
                        t.EPA_COMMENT,
                        t.EPA_PART_ORG,
                        t.EPA_UPDATECOUNT,
                        t.EPA_PK,
                        t.EPA_PARTLOCATION,
                        t.EPA_SYSLEVEL,
                        t.EPA_ASMLEVEL,
                        t.EPA_COMPLEVEL,
                        t.EPA_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ENTITYPARTS as  t
					 union
					        SELECT
                        th.EPA_PART,
                        th.EPA_ENTITY,
                        th.EPA_RENTITY,
                        th.EPA_TYPE,
                        th.EPA_RTYPE,
                        th.EPA_CODE,
                        th.EPA_QTY,
                        th.EPA_UOM,
                        th.EPA_COMMENT,
                        th.EPA_PART_ORG,
                        th.EPA_UPDATECOUNT,
                        th.EPA_PK,
                        th.EPA_PARTLOCATION,
                        th.EPA_SYSLEVEL,
                        th.EPA_ASMLEVEL,
                        th.EPA_COMPLEVEL,
                        th.EPA_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ENTITYPARTS as  th ;
                     

                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DOCENTITIES
                   as                       
                    SELECT
                        t.DAE_DOCUMENT,
                        t.DAE_ENTITY,
                        t.DAE_RENTITY,
                        t.DAE_TYPE,
                        t.DAE_RTYPE,
                        t.DAE_CODE,
                        t.DAE_COPYTOWO,
                        t.DAE_PRINTONWO,
                        t.DAE_COPYTOPO,
                        t.DAE_PRINTONPO,
                        t.DAE_UPDATECOUNT,
                        t.DAE_CREATECOPYTOWO,
                        t.DAE_LASTSAVED,
                        t.DAE_IDMCOPY,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DOCENTITIES as  t
					 union
					        SELECT
                        th.DAE_DOCUMENT,
                        th.DAE_ENTITY,
                        th.DAE_RENTITY,
                        th.DAE_TYPE,
                        th.DAE_RTYPE,
                        th.DAE_CODE,
                        th.DAE_COPYTOWO,
                        th.DAE_PRINTONWO,
                        th.DAE_COPYTOPO,
                        th.DAE_PRINTONPO,
                        th.DAE_UPDATECOUNT,
                        th.DAE_CREATECOPYTOWO,
                        th.DAE_LASTSAVED,
                        th.DAE_IDMCOPY,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DOCENTITIES as  th ;
                     
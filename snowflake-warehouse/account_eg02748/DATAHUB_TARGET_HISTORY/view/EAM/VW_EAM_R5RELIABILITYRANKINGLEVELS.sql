
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5RELIABILITYRANKINGLEVELS
                   as                       
                    SELECT
                        t.RRL_PK,
                        t.RRL_RELIABILITYRANKING,
                        t.RRL_ORG,
                        t.RRL_REVISION,
                        t.RRL_PARENT,
                        t.RRL_CODE,
                        t.RRL_DESC,
                        t.RRL_QUESTIONLEVEL,
                        t.RRL_QUESTION,
                        t.RRL_SEQ,
                        t.RRL_FORMULA,
                        t.RRL_UPDATECOUNT,
                        t.RRL_NUMERIC,
                        t.RRL_INTEGER,
                        t.RRL_MIN,
                        t.RRL_MAX,
                        t.RRL_CALCULATED,
                        t.RRL_QUERYCODE,
                        t.RRL_LASTSAVED,
                        t.RRL_ASPECT,
                        t.RRL_CHECKLISTTYPE,
                        t.RRL_ALLOWOPERATORCHECKLIST,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5RELIABILITYRANKINGLEVELS as  t
					 union
					        SELECT
                        th.RRL_PK,
                        th.RRL_RELIABILITYRANKING,
                        th.RRL_ORG,
                        th.RRL_REVISION,
                        th.RRL_PARENT,
                        th.RRL_CODE,
                        th.RRL_DESC,
                        th.RRL_QUESTIONLEVEL,
                        th.RRL_QUESTION,
                        th.RRL_SEQ,
                        th.RRL_FORMULA,
                        th.RRL_UPDATECOUNT,
                        th.RRL_NUMERIC,
                        th.RRL_INTEGER,
                        th.RRL_MIN,
                        th.RRL_MAX,
                        th.RRL_CALCULATED,
                        th.RRL_QUERYCODE,
                        th.RRL_LASTSAVED,
                        th.RRL_ASPECT,
                        th.RRL_CHECKLISTTYPE,
                        th.RRL_ALLOWOPERATORCHECKLIST,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5RELIABILITYRANKINGLEVELS as  th ;
                     
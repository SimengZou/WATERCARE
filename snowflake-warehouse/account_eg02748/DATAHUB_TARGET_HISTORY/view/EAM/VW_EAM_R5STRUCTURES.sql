
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5STRUCTURES
                   as                       
                    SELECT
                        t.STC_CHILDTYPE,
                        t.STC_CHILDRTYPE,
                        t.STC_CHILD,
                        t.STC_PARENTTYPE,
                        t.STC_PARENTRTYPE,
                        t.STC_PARENT,
                        t.STC_ROLLDOWN,
                        t.STC_ROLLUP,
                        t.STC_EQUIVALENT,
                        t.STC_DOWNTIME,
                        t.STC_CHILD_ORG,
                        t.STC_PARENT_ORG,
                        t.STC_UPDATECOUNT,
                        t.STC_UPDATED,
                        t.STC_SEQUENCE,
                        t.STC_MNBDEFINITION,
                        t.STC_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5STRUCTURES as  t
					 union
					        SELECT
                        th.STC_CHILDTYPE,
                        th.STC_CHILDRTYPE,
                        th.STC_CHILD,
                        th.STC_PARENTTYPE,
                        th.STC_PARENTRTYPE,
                        th.STC_PARENT,
                        th.STC_ROLLDOWN,
                        th.STC_ROLLUP,
                        th.STC_EQUIVALENT,
                        th.STC_DOWNTIME,
                        th.STC_CHILD_ORG,
                        th.STC_PARENT_ORG,
                        th.STC_UPDATECOUNT,
                        th.STC_UPDATED,
                        th.STC_SEQUENCE,
                        th.STC_MNBDEFINITION,
                        th.STC_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5STRUCTURES as  th ;
                     
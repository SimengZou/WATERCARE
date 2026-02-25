
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5RELIABILITYRANKINGRANKS
                   as                       
                    SELECT
                        t.RRR_PK,
                        t.RRR_RELIABILITYRANKING,
                        t.RRR_ORG,
                        t.RRR_REVISION,
                        t.RRR_MINVALUE,
                        t.RRR_MAXVALUE,
                        t.RRR_RRINDEX,
                        t.RRR_UPDATECOUNT,
                        t.RRR_LASTSAVED,
                        t.RRR_CRITICALITY,
                        t.RRR_COLOR,
                        t.RRR_ICON,
                        t.RRR_ICONPATH,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5RELIABILITYRANKINGRANKS as  t
					 union
					        SELECT
                        th.RRR_PK,
                        th.RRR_RELIABILITYRANKING,
                        th.RRR_ORG,
                        th.RRR_REVISION,
                        th.RRR_MINVALUE,
                        th.RRR_MAXVALUE,
                        th.RRR_RRINDEX,
                        th.RRR_UPDATECOUNT,
                        th.RRR_LASTSAVED,
                        th.RRR_CRITICALITY,
                        th.RRR_COLOR,
                        th.RRR_ICON,
                        th.RRR_ICONPATH,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5RELIABILITYRANKINGRANKS as  th ;
                     
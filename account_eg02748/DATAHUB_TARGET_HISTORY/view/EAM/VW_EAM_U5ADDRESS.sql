
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5ADDRESS
                   as                       
                    SELECT
                        t.ADR_KEY,
                        t.ADR_FLAT,
                        t.ADR_HOUSENO,
                        t.ADR_STREET,
                        t.ADR_SUBURB,
                        t.ADR_STATE,
                        t.ADR_POSTALCODE,
                        t.ADR_STREETTYPE,
                        t.ADR_SUBDIVISION,
                        t.ADR_SERVICEAREA,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.LASTSAVED,
                        t.UPDATECOUNT,
                        t.ADR_MISC,
                        t.ADR_ORG,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5ADDRESS as  t
					 union
					        SELECT
                        th.ADR_KEY,
                        th.ADR_FLAT,
                        th.ADR_HOUSENO,
                        th.ADR_STREET,
                        th.ADR_SUBURB,
                        th.ADR_STATE,
                        th.ADR_POSTALCODE,
                        th.ADR_STREETTYPE,
                        th.ADR_SUBDIVISION,
                        th.ADR_SERVICEAREA,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.LASTSAVED,
                        th.UPDATECOUNT,
                        th.ADR_MISC,
                        th.ADR_ORG,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5ADDRESS as  th ;
                     
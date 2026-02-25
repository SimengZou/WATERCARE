
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5WARCOVERAGES
                   as                       
                    SELECT
                        t.WCV_WARRANTY,
                        t.WCV_OBJECT,
                        t.WCV_OBTYPE,
                        t.WCV_OBRTYPE,
                        t.WCV_UOM,
                        t.WCV_DURATION,
                        t.WCV_EXPIRATION,
                        t.WCV_EXPIRATIONDATE,
                        t.WCV_NEARTHRESHOLD,
                        t.WCV_STARTUSAGE,
                        t.WCV_SEQNO,
                        t.WCV_ACTIVE,
                        t.WCV_STARTDATE,
                        t.WCV_RECORDED,
                        t.WCV_USER,
                        t.WCV_OBJECT_ORG,
                        t.WCV_UPDATECOUNT,
                        t.WCV_CREATED,
                        t.WCV_UPDATED,
                        t.WCV_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5WARCOVERAGES as  t
					 union
					        SELECT
                        th.WCV_WARRANTY,
                        th.WCV_OBJECT,
                        th.WCV_OBTYPE,
                        th.WCV_OBRTYPE,
                        th.WCV_UOM,
                        th.WCV_DURATION,
                        th.WCV_EXPIRATION,
                        th.WCV_EXPIRATIONDATE,
                        th.WCV_NEARTHRESHOLD,
                        th.WCV_STARTUSAGE,
                        th.WCV_SEQNO,
                        th.WCV_ACTIVE,
                        th.WCV_STARTDATE,
                        th.WCV_RECORDED,
                        th.WCV_USER,
                        th.WCV_OBJECT_ORG,
                        th.WCV_UPDATECOUNT,
                        th.WCV_CREATED,
                        th.WCV_UPDATED,
                        th.WCV_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5WARCOVERAGES as  th ;
                     
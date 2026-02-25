
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5CIWORESULTS
                   as                       
                    SELECT
                        t.CIR_SEQUENCE,
                        t.CIR_TRANSID,
                        t.CIR_WORKORDERNUM,
                        t.CIR_TYPE,
                        t.CIR_CODE,
                        t.CIR_DETAIL,
                        t.CIR_OBSVALUE,
                        t.CIR_OBSDATE,
                        t.CIR_OBSDESCRIPTION,
                        t.CIR_OBSUOM,
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
          
                     FROM DATAHUB_TARGET.EAM_U5CIWORESULTS as  t
					 union
					        SELECT
                        th.CIR_SEQUENCE,
                        th.CIR_TRANSID,
                        th.CIR_WORKORDERNUM,
                        th.CIR_TYPE,
                        th.CIR_CODE,
                        th.CIR_DETAIL,
                        th.CIR_OBSVALUE,
                        th.CIR_OBSDATE,
                        th.CIR_OBSDESCRIPTION,
                        th.CIR_OBSUOM,
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
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5CIWORESULTS as  th ;
                     
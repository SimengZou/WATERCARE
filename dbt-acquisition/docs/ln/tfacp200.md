# tfacp200

LN tfacp200 Open Items (Purchase Invoices and Payments) table, Generated 2026-04-10T19:41:28Z from package combination ce01101

## Overview

| Property | Value |
|---|---|
| **Source system** | `ln` |
| **Source table** | `ln_tfacp200` |
| **Materialization** | `incremental` |
| **Primary keys** | `compnr`, `ttyp`, `ninv`, `line`, `tdoc`, `docn`, `lino` |
| **Column count** | 274 |

## Columns

| Column | dbt Type | Snowflake Type | Flags | Tests | Description |
|---|---|---|---|---|---|
| `compnr` | `integer` | `int` | `PK` | `not_null` | (required) Company number |
| `ttyp` | `string` | `varchar` | `PK` | `not_null` | (required) Transaction Type. Max length: 3 |
| `ninv` | `integer` | `int` | `PK` | `not_null` | (required) Document |
| `line` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `tdoc` | `string` | `varchar` | `PK` | `not_null` | (required) Transaction Type. Max length: 3 |
| `docn` | `integer` | `int` | `PK` | `not_null` | (required) Document |
| `lino` | `integer` | `int` | `PK` | `not_null` | (required) Line |
| `ifbp` | `string` | `varchar` |  |  | Business Partner. Max length: 9 |
| `isup__en_us` | `string` | `varchar` |  | `not_null` | (required) Supplier's Invoice Number - base language. Max length: 50 |
| `ptbp` | `string` | `varchar` |  |  | Pay-to Business Partner. Max length: 9 |
| `otbp` | `string` | `varchar` |  |  | Buy-from Business Partner. Max length: 9 |
| `docd` | `date` | `date` |  |  | Document Date |
| `tpay` | `integer` | `int` |  |  | Document Type. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 |
| `tpay_kw` | `string` | `varchar` |  |  | Document Type (keyword). Allowed values: tfacp.tpay.invoice, tfacp.tpay.normal, tfacp.tpay.sales, tfacp.tpay.credit, tfacp.tpay.correction, tfacp.tpay.currency, tfacp.tpay.payment, tfacp.tpay.anticipated, tfacp.tpay.advance, tfacp.tpay.unallocated, tfacp.tpay.advance.ant, tfacp.tpay.unallocated.ant, tfacp.tpay.standing, tfacp.tpay.assignment, tfacp.tpay.trade.notes, tfacp.tpay.cash.journal |
| `ccur` | `string` | `varchar` |  |  | Currency. Max length: 3 |
| `pfre` | `integer` | `int` |  |  | Self-Billing/Internal/Automatic Invoice. Allowed values: 0, 1, 2 |
| `pfre_kw` | `string` | `varchar` |  |  | Self-Billing/Internal/Automatic Invoice (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `rate_1` | `float` | `float` |  |  | Currency Rate |
| `rate_2` | `float` | `float` |  |  | Currency Rate |
| `rate_3` | `float` | `float` |  |  | Currency Rate |
| `ratf_1` | `integer` | `int` |  |  | Rate Factor |
| `ratf_2` | `integer` | `int` |  |  | Rate Factor |
| `ratf_3` | `integer` | `int` |  |  | Rate Factor |
| `rade` | `integer` | `int` |  |  | Rate Determination. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8, 10, 20 |
| `rade_kw` | `string` | `varchar` |  |  | Rate Determination (keyword). Allowed values: tcfcrt.fixedl, tcfcrt.fixedh, tcfcrt.fixedlh, tcfcrt.ddat, tcfcrt.rdat, tcfcrt.docd, tcfcrt.excd, tcfcrt.manual, tcfcrt.fixed, tcfcrt.not.appl |
| `amnt` | `float` | `float` |  |  | Amount |
| `ratd` | `timestamp` | `timestamp_ntz` |  |  | Rate Date |
| `rtyp` | `string` | `varchar` |  |  | Exchange Rate Type. Max length: 3 |
| `amti` | `float` | `float` |  |  | Amount in Invoice Currency |
| `amth_1` | `float` | `float` |  |  | Amount in Home Currency |
| `amth_2` | `float` | `float` |  |  | Amount in Home Currency |
| `amth_3` | `float` | `float` |  |  | Amount in Home Currency |
| `vatc` | `string` | `varchar` |  |  | Tax Country. Max length: 3 |
| `cvat` | `string` | `varchar` |  |  | Tax Code. Max length: 9 |
| `vata` | `float` | `float` |  |  | Tax Amount in Payment Currency |
| `vati` | `float` | `float` |  |  | Tax Amount in Invoice Currency |
| `vath_1` | `float` | `float` |  |  | Tax Amount in Home Currency |
| `vath_2` | `float` | `float` |  |  | Tax Amount in Home Currency |
| `vath_3` | `float` | `float` |  |  | Tax Amount in Home Currency |
| `svam` | `float` | `float` |  |  | Shifted Tax |
| `svah_1` | `float` | `float` |  |  | Shifted Tax HC |
| `svah_2` | `float` | `float` |  |  | Shifted Tax HC |
| `svah_3` | `float` | `float` |  |  | Shifted Tax HC |
| `lpdt` | `date` | `date` |  |  | Late Payment Surcharge Date |
| `lapa` | `float` | `float` |  |  | Late Payment Surcharge Amount |
| `lapi` | `float` | `float` |  |  | Late Payment Surcharge Amount in IC |
| `laph_1` | `float` | `float` |  |  | Late Payment Surcharge Amount in HC |
| `laph_2` | `float` | `float` |  |  | Late Payment Surcharge Amount in HC |
| `laph_3` | `float` | `float` |  |  | Late Payment Surcharge Amount in HC |
| `did1` | `date` | `date` |  |  | First Discount Date |
| `dc1a` | `float` | `float` |  |  | First Discount Amount |
| `dc1i` | `float` | `float` |  |  | First Discount Amount in Invoice Currency |
| `dc1h_1` | `float` | `float` |  |  | First Discount Amount in Home Currency |
| `dc1h_2` | `float` | `float` |  |  | First Discount Amount in Home Currency |
| `dc1h_3` | `float` | `float` |  |  | First Discount Amount in Home Currency |
| `did2` | `date` | `date` |  |  | Second Discount Date |
| `dc2a` | `float` | `float` |  |  | Second Discount Amount |
| `dc2i` | `float` | `float` |  |  | Second Discount Amount in Invoice Currency |
| `dc2h_1` | `float` | `float` |  |  | Second Discount Amount in Home Currency |
| `dc2h_2` | `float` | `float` |  |  | Second Discount Amount in Home Currency |
| `dc2h_3` | `float` | `float` |  |  | Second Discount Amount in Home Currency |
| `did3` | `date` | `date` |  |  | Third Discount Date |
| `dc3a` | `float` | `float` |  |  | Third Discount Amount |
| `dc3i` | `float` | `float` |  |  | Third Discount Amount in Invoice Currency |
| `dc3h_1` | `float` | `float` |  |  | Third Discount Amount in Home Currency |
| `dc3h_2` | `float` | `float` |  |  | Third Discount Amount in Home Currency |
| `dc3h_3` | `float` | `float` |  |  | Third Discount Amount in Home Currency |
| `pada` | `float` | `float` |  |  | Payment Difference |
| `padi` | `float` | `float` |  |  | Payment Difference Amount in Invoice Currency |
| `padh_1` | `float` | `float` |  |  | Payment Difference Amount in Home Currency |
| `padh_2` | `float` | `float` |  |  | Payment Difference Amount in Home Currency |
| `padh_3` | `float` | `float` |  |  | Payment Difference Amount in Home Currency |
| `basi` | `float` | `float` |  |  | Currency Diff. Basis |
| `cdam_1` | `float` | `float` |  |  | Currency Difference |
| `cdam_2` | `float` | `float` |  |  | Currency Difference |
| `cdam_3` | `float` | `float` |  |  | Currency Difference |
| `tore_1` | `float` | `float` |  |  | To Realized Currency Differences |
| `tore_2` | `float` | `float` |  |  | To Realized Currency Differences |
| `tore_3` | `float` | `float` |  |  | To Realized Currency Differences |
| `baco` | `float` | `float` |  |  | Bank Charges |
| `baca_1` | `float` | `float` |  |  | Bank Costs in HC |
| `baca_2` | `float` | `float` |  |  | Bank Costs in HC |
| `baca_3` | `float` | `float` |  |  | Bank Costs in HC |
| `appr` | `integer` | `int` |  |  | Match with Orders. Allowed values: 1, 2, 3, 4, 10, 20 |
| `appr_kw` | `string` | `varchar` |  |  | Match with Orders (keyword). Allowed values: tfacp.inv.expence, tfacp.inv.purchase, tfacp.inv.freight, tfacp.inv.na, tfacp.inv.landed, tfacp.inv.services.procm |
| `dued` | `date` | `date` |  |  | Due Date |
| `refr__en_us` | `string` | `varchar` |  | `not_null` | (required) Transaction Reference - base language. Max length: 32 |
| `ccrs` | `string` | `varchar` |  |  | Late Payment Surcharge. Max length: 3 |
| `cpay` | `string` | `varchar` |  |  | Payment Terms. Max length: 3 |
| `otyp` | `string` | `varchar` |  |  | Original Invoice Transaction Type. Max length: 3 |
| `oinv` | `integer` | `int` |  |  | Original Invoice |
| `osch` | `integer` | `int` |  |  | Original Schedule Line Number |
| `subc` | `integer` | `int` |  |  | Subcontracting. Allowed values: 1, 2 |
| `subc_kw` | `string` | `varchar` |  |  | Subcontracting (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `blac` | `integer` | `int` |  |  | Type of Blocked Account. Allowed values: 0, 1, 2, 3, 4 |
| `blac_kw` | `string` | `varchar` |  |  | Type of Blocked Account (keyword). Allowed values: , tfcmg.blac.g.account, tfcmg.blac.iab, tfcmg.blac.tax, tfcmg.blac.na |
| `ragr` | `string` | `varchar` |  |  | Remittance Agreement. Max length: 16 |
| `paym` | `string` | `varchar` |  |  | Payment/Receipt Method. Max length: 3 |
| `loco` | `integer` | `int` |  |  | Logistic Company |
| `orno` | `string` | `varchar` |  |  | Order. Max length: 9 |
| `bloc` | `string` | `varchar` |  |  | Hold Reason. Max length: 3 |
| `bdat` | `date` | `date` |  |  | Approve Before |
| `bref` | `string` | `varchar` |  |  | Assigned Approver. Max length: 3 |
| `bank` | `string` | `varchar` |  |  | Supplier's Bank. Max length: 3 |
| `reas` | `string` | `varchar` |  |  | Reason for Payment. Max length: 3 |
| `post` | `integer` | `int` |  |  | Finalization Run Number |
| `pdat` | `date` | `date` |  |  | Date of Finalization |
| `balc` | `float` | `float` |  |  | Balance |
| `balh_1` | `float` | `float` |  |  | Balance in Home Currency |
| `balh_2` | `float` | `float` |  |  | Balance in Home Currency |
| `balh_3` | `float` | `float` |  |  | Balance in Home Currency |
| `bala` | `float` | `float` |  |  | Balance Anticipated |
| `bahc_1` | `float` | `float` |  |  | Balance Anticipated in HC |
| `bahc_2` | `float` | `float` |  |  | Balance Anticipated in HC |
| `bahc_3` | `float` | `float` |  |  | Balance Anticipated in HC |
| `year` | `integer` | `int` |  |  | Fiscal Year |
| `prod` | `integer` | `int` |  |  | Fiscal Period |
| `btno` | `integer` | `int` |  |  | Batch |
| `link` | `float` | `float` |  |  | Matched Amount |
| `pdoc` | `string` | `varchar` |  |  | Check Number. Max length: 15 |
| `stap` | `integer` | `int` |  |  | Invoice Status. Allowed values: 1, 2, 3, 4 |
| `stap_kw` | `string` | `varchar` |  |  | Invoice Status (keyword). Allowed values: tfacp.stap.registered, tfacp.stap.transactions, tfacp.stap.linked, tfacp.stap.approved |
| `user` | `string` | `varchar` |  |  | User. Max length: 16 |
| `vaty` | `integer` | `int` |  |  | Tax Year |
| `vatp` | `integer` | `int` |  |  | Tax Period |
| `tapr` | `string` | `varchar` |  |  | Transaction Type Approval. Max length: 3 |
| `pdif` | `float` | `float` |  |  | Price Difference |
| `papr` | `integer` | `int` |  |  | Invoice Approved. Allowed values: 1, 2 |
| `papr_kw` | `string` | `varchar` |  |  | Invoice Approved (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `dapr` | `integer` | `int` |  |  | Document Approval |
| `liqd` | `date` | `date` |  |  | Expected Liquidity Date |
| `step` | `integer` | `int` |  |  | Payment Procedure Step. Allowed values: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 |
| `step_kw` | `string` | `varchar` |  |  | Payment Procedure Step (keyword). Allowed values: , tfcmg.step.doc.selected, tfcmg.step.doc.open, tfcmg.step.doc.received, tfcmg.step.doc.issued, tfcmg.step.doc.to.cust, tfcmg.step.doc.acc.by.cust, tfcmg.step.doc.acc.sent, tfcmg.step.doc.collateral, tfcmg.step.doc.sent.disc, tfcmg.step.doc.to.bank, tfcmg.step.doc.endorsed, tfcmg.step.doc.discounted, tfcmg.step.doc.cancelled, tfcmg.step.doc.matured, tfcmg.step.doc.void, tfcmg.step.doc.paid, tfcmg.step.doc.rejected, tfcmg.step.doc.dishonored, tfcmg.step.doc.settled, tfcmg.step.empty |
| `rcpt` | `integer` | `int` |  |  | Status for Linking Unallocated Payment. Allowed values: 0, 1, 2, 3, 4, 5, 10 |
| `rcpt_kw` | `string` | `varchar` |  |  | Status for Linking Unallocated Payment (keyword). Allowed values: , tfcmg.rcpt.automatic, tfcmg.rcpt.no, tfcmg.rcpt.manual, tfcmg.rcpt.cash.recp, tfcmg.rcpt.expense, tfcmg.rcpt.distribution |
| `lvat` | `integer` | `int` |  |  | Level of Tax Calculation. Allowed values: 0, 1, 2, 3 |
| `lvat_kw` | `string` | `varchar` |  |  | Level of Tax Calculation (keyword). Allowed values: , tfacp.vatc.header, tfacp.vatc.transaction, tfacp.vatc.not.applicable |
| `typa` | `string` | `varchar` |  |  | Anticipated Transaction Type. Max length: 3 |
| `doca` | `integer` | `int` |  |  | Anticipated Document Number |
| `shpm__en_us` | `string` | `varchar` |  | `not_null` | (required) Packing Slip - base language. Max length: 30 |
| `leac` | `string` | `varchar` |  |  | Control Account. Max length: 12 |
| `dim1` | `string` | `varchar` |  |  | Dimension 1. Max length: 9 |
| `dim2` | `string` | `varchar` |  |  | Dimension 2. Max length: 9 |
| `dim3` | `string` | `varchar` |  |  | Dimension 3. Max length: 9 |
| `dim4` | `string` | `varchar` |  |  | Dimension 4. Max length: 9 |
| `dim5` | `string` | `varchar` |  |  | Dimension 5. Max length: 9 |
| `dim6` | `string` | `varchar` |  |  | Dimension 6. Max length: 9 |
| `dim7` | `string` | `varchar` |  |  | Dimension 7. Max length: 9 |
| `dim8` | `string` | `varchar` |  |  | Dimension 8. Max length: 9 |
| `dim9` | `string` | `varchar` |  |  | Dimension 9. Max length: 9 |
| `dm10` | `string` | `varchar` |  |  | Dimension 10. Max length: 9 |
| `dm11` | `string` | `varchar` |  |  | Dimension 11. Max length: 9 |
| `dm12` | `string` | `varchar` |  |  | Dimension 12. Max length: 9 |
| `load` | `string` | `varchar` |  |  | Load. Max length: 9 |
| `shpi` | `string` | `varchar` |  |  | Shipment. Max length: 9 |
| `adty` | `integer` | `int` |  |  | Category. Allowed values: 0, 1, 2, 3, 4, 10 |
| `adty_kw` | `string` | `varchar` |  |  | Category (keyword). Allowed values: , tfcmg.adty.tang.assets, tfcmg.adty.intang.assets, tfcmg.adty.inven, tfcmg.adty.other.assets, tfcmg.adty.not.appl |
| `clus` | `string` | `varchar` |  |  | Freight Order Cluster. Max length: 9 |
| `bkrn` | `string` | `varchar` |  |  | Bank Reference. Max length: 27 |
| `ptyp` | `string` | `varchar` |  |  | Purchase Type. Max length: 6 |
| `optb` | `string` | `varchar` |  |  | Original Pay-to Business Partner. Max length: 9 |
| `whti` | `integer` | `int` |  |  | Withholding Tax Invoice. Allowed values: 1, 2 |
| `whti_kw` | `string` | `varchar` |  |  | Withholding Tax Invoice (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `wtpi` | `float` | `float` |  |  | Withholding Tax Paid in Invoice Currency |
| `wtph_1` | `float` | `float` |  |  | Withholding Tax Paid in HC |
| `wtph_2` | `float` | `float` |  |  | Withholding Tax Paid in HC |
| `wtph_3` | `float` | `float` |  |  | Withholding Tax Paid in HC |
| `adcd` | `string` | `varchar` |  |  | Additional description code. Max length: 3 |
| `tnpn` | `string` | `varchar` |  |  | Trade Note Number. Max length: 9 |
| `cfrs` | `string` | `varchar` |  |  | Cash Flow Reason. Max length: 6 |
| `paya` | `string` | `varchar` |  |  | Payment Agreement. Max length: 3 |
| `stat` | `integer` | `int` |  |  | Statistics Updated. Allowed values: 1, 2 |
| `stat_kw` | `string` | `varchar` |  |  | Statistics Updated (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pysh` | `integer` | `int` |  |  | Payments by Schedule. Allowed values: 1, 2 |
| `pysh_kw` | `string` | `varchar` |  |  | Payments by Schedule (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `schn` | `integer` | `int` |  |  | Schedule Line Number |
| `afpy` | `integer` | `int` |  |  | Approved for Payment. Allowed values: 1, 2, 3 |
| `afpy_kw` | `string` | `varchar` |  |  | Approved for Payment (keyword). Allowed values: tcynna.yes, tcynna.no, tcynna.not.app |
| `ildp` | `integer` | `int` |  |  | Invoice Origin. Allowed values: 1, 2, 3, 4, 5, 6, 7, 8 |
| `ildp_kw` | `string` | `varchar` |  |  | Invoice Origin (keyword). Allowed values: tfacp.ildp.self.billed, tfacp.ildp.automatic, tfacp.ildp.not.applicable, tfacp.ildp.internal, tfacp.ildp.external, tfacp.ildp.received.inv, tfacp.ildp.cert.of.paym, tfacp.ildp.commission.inv |
| `prin` | `integer` | `int` |  |  | Invoice printed. Allowed values: 1, 2 |
| `prin_kw` | `string` | `varchar` |  |  | Invoice printed (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `autm` | `integer` | `int` |  |  | Obsolete. Allowed values: 1, 2 |
| `autm_kw` | `string` | `varchar` |  |  | Obsolete (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `pcom` | `integer` | `int` |  |  | Payment Company |
| `payt` | `string` | `varchar` |  |  | Transaction Type. Max length: 3 |
| `payd` | `integer` | `int` |  |  | Document |
| `payl` | `integer` | `int` |  |  | Line |
| `bust` | `integer` | `int` |  |  | Budget Status. Allowed values: 1, 2, 3, 10 |
| `bust_kw` | `string` | `varchar` |  |  | Budget Status (keyword). Allowed values: tfacp.bust.exception, tfacp.bust.not.checked, tfacp.bust.ok, tfacp.bust.not.applicable |
| `sgtp` | `integer` | `int` |  |  | Segment Type. Allowed values: 10, 20, 99 |
| `sgtp_kw` | `string` | `varchar` |  |  | Segment Type (keyword). Allowed values: tfsgtp.header, tfsgtp.line, tfsgtp.na |
| `iadr` | `string` | `varchar` |  |  | Invoice Address. Max length: 9 |
| `tbrl` | `float` | `float` |  |  | Amount to be Relieved in Local Curr. |
| `bdsp` | `integer` | `int` |  |  | Suppress. Allowed values: 0, 1, 2 |
| `bdsp_kw` | `string` | `varchar` |  |  | Suppress (keyword). Allowed values: , tcyesno.yes, tcyesno.no |
| `irdt` | `date` | `date` |  |  | Invoice Receipt Date |
| `vrsm` | `string` | `varchar` |  |  | Variable Symbol. Max length: 10 |
| `txdt` | `date` | `date` |  |  | Tax Date |
| `osup__en_us` | `string` | `varchar` |  | `not_null` | (required) Original Document Number - base language. Max length: 50 |
| `enia` | `integer` | `int` |  |  | Exclude from Net Invoice Amount. Allowed values: 1, 2 |
| `enia_kw` | `string` | `varchar` |  |  | Exclude from Net Invoice Amount (keyword). Allowed values: tcyesno.yes, tcyesno.no |
| `edtp` | `integer` | `int` |  |  | External Document Type. Allowed values: 10, 20, 30, 40, 255 |
| `edtp_kw` | `string` | `varchar` |  |  | External Document Type (keyword). Allowed values: tfacp.edtp.fiscal.receipt, tfacp.edtp.bde, tfacp.edtp.edi, tfacp.edtp.inv.automation, tfacp.edtp.not.applicable |
| `regc` | `string` | `varchar` |  |  | Registration Code. Max length: 9 |
| `bpri` | `string` | `varchar` |  |  | BP Identification Number. Max length: 35 |
| `ptad` | `string` | `varchar` |  |  | Pay-to Address. Max length: 9 |
| `text` | `integer` | `int` |  |  | Text |
| `txtb` | `integer` | `int` |  |  | Text |
| `cdf_plan` | `date` | `date` |  |  | Planned Payment Date |
| `ttyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `tdoc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `ifbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `ptbp_bank_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom125 Bank Accounts by Pay-to Business Partner |
| `ptbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `otbp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom120 Buy-from Business Partners |
| `ccur_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs002 Currencies |
| `vatc_cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs036 Tax Codes by Country |
| `vatc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs010 Countries |
| `cvat_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs037 Tax Codes |
| `ccrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs011 Late Payment Surcharges |
| `cpay_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs013 Payment Terms |
| `otyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `paym_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfcmg003 Payment/Receipt Methods |
| `bloc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfacp002 Hold Reasons |
| `bref_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfacp003 Assigned Approvers |
| `reas_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfcmg002 Reasons for Payment |
| `typa_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld011 Transaction Types |
| `leac_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tfgld008 Chart of Accounts |
| `ptyp_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs201 Purchase Types |
| `optb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom100 Business Partners |
| `cfrs_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs005 Reasons |
| `paya_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tcmcs206 Payment Agreements |
| `iadr_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `regc_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tctax221 Jurisdictions by Registration |
| `ptad_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tccom130 Addresses |
| `text_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `txtb_ref_compnr` | `integer` | `int` |  |  | company number of primary key of table tttxt001 Language Independent Text Data |
| `amth_rfrc` | `float` | `float` |  |  | CC: Amount in Reference Currency |
| `amth_dtwc` | `float` | `float` |  |  | CC: Amount in Data Warehouse Currency |
| `vath_rfrc` | `float` | `float` |  |  | CC: Tax Amount in Reference Currency |
| `vath_dtwc` | `float` | `float` |  |  | CC: Tax Amount in Data Warehouse Currency |
| `laph_rfrc` | `float` | `float` |  |  | CC: Late Payment Surcharge Amount in Reference Currency |
| `laph_dtwc` | `float` | `float` |  |  | CC: Late Payment Surcharge Amount in Data Warehouse Currency |
| `dc1h_rfrc` | `float` | `float` |  |  | CC: First Discount Amount in Reference Currency |
| `dc1h_dtwc` | `float` | `float` |  |  | CC: First Discount Amount in Data Warehouse Currency |
| `dc2h_rfrc` | `float` | `float` |  |  | CC: Second Discount Amount in Reference Currency |
| `dc2h_dtwc` | `float` | `float` |  |  | CC: Second Discount Amount in Data Warehouse Currency |
| `dc3h_rfrc` | `float` | `float` |  |  | CC: Third Discount Amount in Reference Currency |
| `dc3h_dtwc` | `float` | `float` |  |  | CC: Third Discount Amount in Data Warehouse Currency |
| `padh_rfrc` | `float` | `float` |  |  | CC: Payment Difference Amount in Reference Currency |
| `padh_dtwc` | `float` | `float` |  |  | CC: Payment Difference Amount in Data Warehouse Currency |
| `cdam_rfrc` | `float` | `float` |  |  | CC: Currency Difference in Reference Currency |
| `cdam_dtwc` | `float` | `float` |  |  | CC: Currency Difference in Data Warehouse Currency |
| `cdam_invc` | `float` | `float` |  |  | CC: Currency Difference in Invoice Currency |
| `tore_rfrc` | `float` | `float` |  |  | CC: To Realized Currency Differences in Reference Currency |
| `tore_dtwc` | `float` | `float` |  |  | CC: To Realized Currency Differences in Data Warehouse Currency |
| `tore_invc` | `float` | `float` |  |  | CC: To Realized Currency Differences in Invoice Currency |
| `baca_rfrc` | `float` | `float` |  |  | CC: Bank Costs in Reference Currency |
| `baca_dtwc` | `float` | `float` |  |  | CC: Bank Costs in Data Warehouse Currency |
| `baca_invc` | `float` | `float` |  |  | CC: Bank Costs in Invoice Currency |
| `balh_rfrc` | `float` | `float` |  |  | CC: Balance in Reference Currency |
| `balh_dtwc` | `float` | `float` |  |  | CC: Balance in Data Warehouse Currency |
| `bahc_rfrc` | `float` | `float` |  |  | CC: Balance Anticipated in Reference Currency |
| `bahc_dtwc` | `float` | `float` |  |  | CC: Balance Anticipated in Data Warehouse Currency |
| `deleted` | `boolean` | `boolean` |  | `not_null` | (required) Whether record is deleted |
| `username` | `string` | `varchar` |  | `not_null` | (required) User responsible for record action. Max length: 8 |
| `timestamp` | `timestamp` | `timestamp_ntz` |  | `not_null` | (required) Time the action occurred |
| `sequencenumber` | `integer` | `int` |  | `not_null` | (required) Sequence number of the action |
| `etl_load_datetime` | `datetime` | `timestamp_ntz` | `ADF` | | The datetime in which the ETL process loaded the data into the curated layer |
| `etl_load_metadatafilename` | `string` | `varchar` | `ADF` | | The filename of the metadata file that was used to load the data into the curated layer. |
| `dbt_record_updated_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was last updated by dbt. |
| `dbt_record_created_at` | `datetime` | `timestamp_ntz` | `dbt` | | The datetime in which the record was created by dbt. |
| `dbt_invocation_id` | `string` | `varchar` | `dbt` | | The invocation ID of the dbt run that created the record. |
| `dbt_surrogate_key` | `string` | `varchar` | `dbt` | | Hashed key based on primary key/s of table, or all keys excluding dbt and etl metadata. |

--patient_x_medicare_month.SQL  below

WITH bene_status AS (
    SELECT DISTINCT
          org_id
        , pk_bene_id AS fk_patient_id
        , replace(load_period, 's', 'm') AS month_cd_temp
        , src_bene_mdcr_stus_cd AS bene_medicare_status_cd
        , src_bene_dual_stus_cd AS bene_dual_status_cd
        , src_bene_orgnl_entlmt_rsn_cd AS bene_entitlement_reason_cd
        , src_bene_entlmt_buyin_ind AS bene_entitlement_buyin_cd
        , eff_start_dt
        , eff_end_dt
        , record_status_cd
        , CASE 
            WHEN SRC_BENE_PART_A_ENRLMT_BGN_DT >  SRC_BENE_PART_B_ENRLMT_BGN_DT 
                THEN SRC_BENE_PART_A_ENRLMT_BGN_DT
                    ELSE SRC_BENE_PART_B_ENRLMT_BGN_DT
            END AS bene_coverage_start
        --dc eligibility criteria: must have both part a and b coverage and live in the U.S.
        , CASE
            -- 1 = ESRD
            WHEN src_bene_mdcr_stus_cd IN ('11','21','31') 
                AND src_bene_entlmt_buyin_ind IN ('3','C')
                THEN '1'
            -- 2 = Disabled
            WHEN src_bene_mdcr_stus_cd IN ('20') 
                AND src_bene_entlmt_buyin_ind IN ('3','C')
                THEN '2'
            -- 3 = Aged DUAL
            WHEN src_bene_mdcr_stus_cd IN ('10') 
                AND src_bene_dual_stus_cd IN ('01','02','04','08') 
                AND src_bene_entlmt_buyin_ind IN ('3','C')
                THEN '3'
            -- 4 = Aged Non-DUAL
            WHEN src_bene_mdcr_stus_cd IN ('10') 
                AND src_bene_dual_stus_cd NOT IN ('01','02','04','08') 
                --AND src_bene_entlmt_buyin_ind IN ('3','C')     ---Remove this condition????????
                THEN '4'
                    ELSE '#NA'
          END AS assgn_medicare_status_cd
        -- if row_num exceeds 1, then bene eligibility status changed some time during PY
        , ROW_NUMBER() OVER (PARTITION BY pk_bene_id ORDER BY load_period ASC ) AS row_num
    FROM ODS.CCLF_8_BENE_DEMO
    WHERE record_status_cd in ('a','i') 
)
SELECT
      org_id
    , fk_patient_id
    , coalesce(m1.value, m2.value, month_cd_temp) as month_cd 
    , bene_medicare_status_cd
    , bene_dual_status_cd
    , bene_entitlement_reason_cd
    , bene_entitlement_buyin_cd
    , assgn_medicare_status_cd
    , '{{dag_run.conf.load_period}}' AS load_period
    , 1234 AS load_run_id
    , CURRENT_TIMESTAMP AS load_ts
FROM bene_status
-- backfill medicare eligibility status on historical data
LEFT JOIN prod_common.ref.code_month m1
    ON 'm-2018-01' <= m1.value -- historical alignment year data begins at CY2018+
        AND 'm-2021-12' >= m1.value
        AND bene_status.row_num = 1   
        AND to_char(bene_status.EFF_END_DT, 'm-YYYY-MM') > m1.value
        AND m1.value > TO_CHAR(bene_status.bene_coverage_start, 'm-YYYY-MM')
 LEFT JOIN prod_common.ref.code_month m2
    on 
--record_status_cd ='a'   ---Remove this filter????
--    AND 
    to_char(bene_status.EFF_END_DT, 'm-YYYY-MM') > m2.value
    AND m2.value > TO_CHAR(bene_status.bene_coverage_start, 'm-YYYY-MM')
    AND m2.value <= 'm-2021-12'
    AND m2.value >= to_char(bene_status.EFF_start_DT,'m-YYYY-MM')
    AND bene_status.row_num != 1 
WHERE month_cd = 'm-2021-10'   --the testing month
AND assgn_medicare_status_cd in ('1','2','3','4')
    ;
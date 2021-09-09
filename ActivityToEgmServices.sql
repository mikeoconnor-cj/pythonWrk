--EGM Services
SELECT SPLIT_PART(PK_ACTIVITY_ID,'|', 3) AS clm_id 
	, SPLIT_PART(PK_ACTIVITY_ID,'|', 4) AS line_num
	, FK_PATIENT_ID AS bene_key ---for Change, there's a hash for a2024, there's an org_src_id prefix
	, CASE 
		WHEN regexp_like(PROCEDURE_HCPCS_MOD_CD_LIST[0],'E[1-4]|FA|F[1-9]|L[CDT]|R[CT]|TA|T[1-9]|50') THEN trim(PROCEDURE_HCPCS_MOD_CD_LIST[0])
		WHEN regexp_like(PROCEDURE_HCPCS_MOD_CD_LIST[1],'E[1-4]|FA|F[1-9]|L[CDT]|R[CT]|TA|T[1-9]|50') THEN trim(PROCEDURE_HCPCS_MOD_CD_LIST[1])
		WHEN regexp_like(PROCEDURE_HCPCS_MOD_CD_LIST[2],'E[1-4]|FA|F[1-9]|L[CDT]|R[CT]|TA|T[1-9]|50') THEN trim(PROCEDURE_HCPCS_MOD_CD_LIST[2])
	  END AS body_mod
	, trim(PROCEDURE_HCPCS_MOD_CD_LIST[0]) AS mod_1
	, trim(PROCEDURE_HCPCS_MOD_CD_LIST[1]) AS mod_2
	, CASE
		WHEN activity_type_cd = 'phys' THEN claim_line_allowed_amt
		WHEN activity_type_cd in ('fac_rev', 'dme') THEN CLAIM_LINE_PAID_AMT 
		WHEN activity_type_cd = 'med' THEN claim_line_bene_paid_amt
		ELSE 0
	  END AS pay
	, FACILITY_PLACE_OF_SERVICE_CD AS pos_cd
	, FACILITY_NPI_NUM AS prf_at_grp_npi
	, SPLIT_PART(fk_provider_primary_id,'|',2) AS prf_op_physn_npi  --etls show primary ID is best as it already considers claim type
	, split_part(fk_diagnosis_id_list[0],'|',2 )AS primary_dx
	, PROVIDER_RENDERING_SPECIALTY_CD AS prov_spec
	, CASE
		WHEN claim_type_cd IN ('40', '60',  '61') THEN facility_ccn_num
		WHEN claim_type_cd = '10' THEN facility_ccn_num
		ELSE SPLIT_PART(fk_provider_primary_id,'|',2)
	  END AS providers
	, FACILITY_REVENUE_CENTER_CD AS rev_cd
	, '' AS rfr_pt_physn_npi
	, CASE 
		WHEN claim_type_cd NOT IN ('70','71') THEN SPLIT_PART(PK_ACTIVITY_ID,'|', 4)
	  END AS sgmt_num	
	, CASE 
		WHEN claim_type_cd = '40' THEN 'op'
		WHEN claim_type_cd IN ('70','71') THEN 'pb'
		WHEN claim_type_cd IN ('60', '61') THEN 'ip'
		WHEN claim_type_cd = '10' THEN 'hh'
		-- other cases '01' '02' is med
	  END AS src_file	
	, CASE 
		WHEN ACTIVITY_TYPE_CD = 'fac_proc'
			THEN coalesce(NULLIF(PROCEDURE_ICD_9_CD,'#NA'), NULLIF(PROCEDURE_ICD_9_CD,'#NA'))
		WHEN ACTIVITY_TYPE_CD = 'med'
			THEN MEDICATION_NDC_SPL_CD	--is MEDICATION_HCPCS_CD useful? is it used in Services?
		WHEN ACTIVITY_TYPE_CD = 'dme'	
			THEN DME_HCPCS_CD
		ELSE PROCEDURE_HCPCS_CD 			
	  END AS src_code	
	, activity_from_dt AS svc_from_dt
	, activity_thru_dt AS svc_thru_dt
FROM insights.ACTIVITY 
WHERE 
ACTIVITY_TYPE_CD NOT in ('med', 'dme')
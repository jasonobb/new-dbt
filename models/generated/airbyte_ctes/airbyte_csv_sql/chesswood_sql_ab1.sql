{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_airbyte_csv_sql",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('airbyte_csv_sql', '_airbyte_raw_chesswood_sql') }}
select
    {{ json_extract_scalar('_airbyte_data', ['lease_num'], ['lease_num']) }} as lease_num,
    {{ json_extract_scalar('_airbyte_data', ['lease_term'], ['lease_term']) }} as lease_term,
    {{ json_extract_scalar('_airbyte_data', ['lease_type'], ['lease_type']) }} as lease_type,
    {{ json_extract_scalar('_airbyte_data', ['lease_yield'], ['lease_yield']) }} as lease_yield,
    {{ json_extract_scalar('_airbyte_data', ['lease_app_num'], ['lease_app_num']) }} as lease_app_num,
    {{ json_extract_scalar('_airbyte_data', ['lease_comp_num'], ['lease_comp_num']) }} as lease_comp_num,
    {{ json_extract_scalar('_airbyte_data', ['lease_pmts_rem'], ['lease_pmts_rem']) }} as lease_pmts_rem,
    {{ json_extract_scalar('_airbyte_data', ['lease_loan_rate'], ['lease_loan_rate']) }} as lease_loan_rate,
    {{ json_extract_scalar('_airbyte_data', ['lease_sign_date'], ['lease_sign_date']) }} as lease_sign_date,
    {{ json_extract_scalar('_airbyte_data', ['lease_term_date'], ['lease_term_date']) }} as lease_term_date,
    {{ json_extract_scalar('_airbyte_data', ['lease_book_value'], ['lease_book_value']) }} as lease_book_value,
    {{ json_extract_scalar('_airbyte_data', ['lease_booked_idc'], ['lease_booked_idc']) }} as lease_booked_idc,
    {{ json_extract_scalar('_airbyte_data', ['lease_equip_cost'], ['lease_equip_cost']) }} as lease_equip_cost,
    {{ json_extract_scalar('_airbyte_data', ['lease_equip_type'], ['lease_equip_type']) }} as lease_equip_type,
    {{ json_extract_scalar('_airbyte_data', ['lease_pmts_final'], ['lease_pmts_final']) }} as lease_pmts_final,
    {{ json_extract_scalar('_airbyte_data', ['lease_pmts_invcd'], ['lease_pmts_invcd']) }} as lease_pmts_invcd,
    {{ json_extract_scalar('_airbyte_data', ['lease_start_date'], ['lease_start_date']) }} as lease_start_date,
    {{ json_extract_scalar('_airbyte_data', ['lease_total_pmts'], ['lease_total_pmts']) }} as lease_total_pmts,
    {{ json_extract_scalar('_airbyte_data', ['lease_vendor_num'], ['lease_vendor_num']) }} as lease_vendor_num,
    {{ json_extract_scalar('_airbyte_data', ['lease_billto_city'], ['lease_billto_city']) }} as lease_billto_city,
    {{ json_extract_scalar('_airbyte_data', ['lease_cust_id_num'], ['lease_cust_id_num']) }} as lease_cust_id_num,
    {{ json_extract_scalar('_airbyte_data', ['lease_delinq_code'], ['lease_delinq_code']) }} as lease_delinq_code,
    {{ json_extract_scalar('_airbyte_data', ['lease_implicit_rate'], ['lease_implicit_rate']) }} as lease_implicit_rate,
    {{ json_extract_scalar('_airbyte_data', ['lease_maturity_date'], ['lease_maturity_date']) }} as lease_maturity_date,
    {{ json_extract_scalar('_airbyte_data', ['lease_pay_frequency'], ['lease_pay_frequency']) }} as lease_pay_frequency,
    {{ json_extract_scalar('_airbyte_data', ['lease_pmts_received'], ['lease_pmts_received']) }} as lease_pmts_received,
    {{ json_extract_scalar('_airbyte_data', ['lease_purch_opt_amt'], ['lease_purch_opt_amt']) }} as lease_purch_opt_amt,
    {{ json_extract_scalar('_airbyte_data', ['lease_acct_dist_code'], ['lease_acct_dist_code']) }} as lease_acct_dist_code,
    {{ json_extract_scalar('_airbyte_data', ['lease_booked_eq_cost'], ['lease_booked_eq_cost']) }} as lease_booked_eq_cost,
    {{ json_extract_scalar('_airbyte_data', ['lease_pmts_rem_invcd'], ['lease_pmts_rem_invcd']) }} as lease_pmts_rem_invcd,
    {{ json_extract_scalar('_airbyte_data', ['lease_rent_tran_code'], ['lease_rent_tran_code']) }} as lease_rent_tran_code,
    {{ json_extract_scalar('_airbyte_data', ['lease_booked_residual'], ['lease_booked_residual']) }} as lease_booked_residual,
    {{ json_extract_scalar('_airbyte_data', ['lease_county_tax_code'], ['lease_county_tax_code']) }} as lease_county_tax_code,
    {{ json_extract_scalar('_airbyte_data', ['lease_oldest_rent_due'], ['lease_oldest_rent_due']) }} as lease_oldest_rent_due,
    {{ json_extract_scalar('_airbyte_data', ['lease_booked_post_date'], ['lease_booked_post_date']) }} as lease_booked_post_date,
    {{ json_extract_scalar('_airbyte_data', ['lease_interim_rent_amt'], ['lease_interim_rent_amt']) }} as lease_interim_rent_amt,
    {{ json_extract_scalar('_airbyte_data', ['lease_booked_receivable'], ['lease_booked_receivable']) }} as lease_booked_receivable,
    {{ json_extract_scalar('_airbyte_data', ['lease_contract_mgr_code'], ['lease_contract_mgr_code']) }} as lease_contract_mgr_code,
    {{ json_extract_scalar('_airbyte_data', ['lease_next_inv_due_date'], ['lease_next_inv_due_date']) }} as lease_next_inv_due_date,
    {{ json_extract_scalar('_airbyte_data', ['lease_total_monthly_pmt'], ['lease_total_monthly_pmt']) }} as lease_total_monthly_pmt,
    {{ json_extract_scalar('_airbyte_data', ['contract_finance_type_id'], ['contract_finance_type_id']) }} as contract_finance_type_id,
    {{ json_extract_scalar('_airbyte_data', ['lease_earn_to_date_lease'], ['lease_earn_to_date_lease']) }} as lease_earn_to_date_lease,
    {{ json_extract_scalar('_airbyte_data', ['lease_extended_matur_date'], ['lease_extended_matur_date']) }} as lease_extended_matur_date,
    {{ json_extract_scalar('_airbyte_data', ['customer_business_indivdual_indicator'], ['customer_business_indivdual_indicator']) }} as customer_business_indivdual_indicator,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('airbyte_csv_sql', '_airbyte_raw_chesswood_sql') }} as table_alias
-- chesswood_sql
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}


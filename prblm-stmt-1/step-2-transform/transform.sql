
WITH schedules_data AS (
    SELECT 
        loan_id,
        ROUND(expected_payment_amount,2) AS scheduled_amount,
        date(expected_payment_date) AS scheduled_date,
        ROW_NUMBER() OVER (
            PARTITION BY loan_id 
            ORDER BY expected_payment_date DESC) AS row_num,
        ROW_NUMBER() OVER (
            PARTITION BY loan_id 
            ORDER BY expected_payment_date ASC) AS schedule_num,
        SUM(ROUND(expected_payment_amount,2)) OVER (
            PARTITION BY loan_id 
            ORDER BY expected_payment_date ASC) AS running_total_expected
    FROM payment_schedules
), 

payments_data AS (
    SELECT 
        loan_id,
        DATE(date_paid) AS date_paid,
        ROUND(amount_paid,2) AS amount_paid,
        ROW_NUMBER() OVER (
            PARTITION BY loan_id 
            ORDER BY date_paid DESC) AS row_num,
        ROW_NUMBER() OVER (
            PARTITION BY loan_id 
            ORDER BY date_paid ASC) AS payment_num,
        SUM(ROUND(amount_paid,2)) OVER (
            PARTITION BY loan_id 
            ORDER BY date_paid ASC) AS running_total_paid
    FROM loan_payments
),

/*
if last_payment_date before latest_scheduled_date 
then DPD = date_diff(current_date, latest_scheduled_date) 
else DPD = 0
does not consider advance payments 
coniders partial payments to reset DPD
*/
dpd AS (
    WITH latest_schedules AS (
        SELECT loan_id, scheduled_date 
        FROM schedules_data 
        WHERE row_num = 1 --latest schedule
    ),

    latest_payments AS (
        SELECT loan_id, date_paid
        FROM payments_data
        WHERE row_num = 1 --latest payment
    )

    SELECT
        latest_schedules.loan_id,
        latest_schedules.scheduled_date AS last_due_date,
        latest_payments.date_paid AS last_repayment_date,
        CASE 
            WHEN latest_payments.date_paid < latest_schedules.scheduled_date
            THEN julianday('now') - julianday(latest_schedules.scheduled_date)
            ELSE 0
        END AS current_days_past_due

    FROM latest_schedules
    LEFT JOIN latest_payments 
        ON latest_payments.loan_id = latest_schedules.loan_id
),

/*
par days - the number of days the loan was not paid in full
*/
par_days_amt_risk AS (
    WITH running_totals AS (
        SELECT
            sd.loan_id,
            sd.schedule_num,
            sd.scheduled_date,
            sd.running_total_expected,
            pd.payment_num,
            pd.date_paid,
            pd.running_total_paid,
            --pd.running_total_paid - sd.running_total_expected AS bonus_amount_paid,
            LAG(pd.date_paid) OVER(
                PARTITION BY pd.loan_id
                ORDER BY pd.payment_num ASC) AS prev_date_paid
        FROM schedules_data AS sd
        LEFT JOIN payments_data AS pd
            ON pd.loan_id = sd.loan_id
            AND pd.payment_num = sd.schedule_num
        -- do not consider future schedules
        WHERE sd.scheduled_date < DATE('now')
    )

    SELECT *,
    -- amount at risk
    running_total_expected - running_total_paid AS amount_risk,
    CASE 
        -- SCENARIO 1: first time payments
        WHEN prev_date_paid IS NULL 
        THEN julianday(date_paid) - julianday(scheduled_date) 
        -- for subsequent payments
        -- SCENARIO 2: paid on schedule 
        -- and in full
        -- no debt days
        WHEN date_paid = scheduled_date
        AND running_total_paid >= running_total_expected
        THEN 0 
        -- SCENARIO 3: paid later than schedule
        -- and not from a previous payment
        -- and in full
        -- calculate debt days
        WHEN date_paid > scheduled_date
        AND date_paid != prev_date_paid
        AND running_total_paid >= running_total_expected
        THEN julianday(date_paid) - julianday(scheduled_date)
        -- SCENARIO 4: paid earlier than schedule (lumpsum)
        -- and covers this month's expected in full
        -- no debt days
        WHEN date_paid == prev_date_paid
        AND running_total_paid >= running_total_expected
        THEN 0
    END AS par_days,
    -- to get last amount at risk
    ROW_NUMBER() OVER(PARTITION BY loan_id ORDER BY schedule_num DESC) AS final_schedule_num
    FROM running_totals
),

final_par_days AS (
    SELECT loan_id, SUM(par_days) AS par_days
    FROM par_days_amt_risk
    GROUP BY 1
),


final_amt_risk AS (
    SELECT loan_id, amount_risk 
    FROM par_days_amt_risk
    WHERE final_schedule_num = 1 -- point in time, final schedule
),

loan_amounts AS (
    WITH amounts_expected AS (
        SELECT 
            loan_id, 
            SUM(ROUND(expected_payment_amount,2)) AS total_amount_expected
        FROM payment_schedules
        -- expected payment date has passed
        WHERE DATE(expected_payment_date) < DATE('now')
        GROUP BY 1
    ),

    amounts_paid AS (
        SELECT
            loan_id, 
            SUM(ROUND(amount_paid,2)) AS total_amount_paid
        FROM loan_payments
        -- exclude "future-dated" payments ?
        -- TODO ask about this. . 
        WHERE DATE(date_paid) < DATE('now')
        GROUP BY 1
    )

    SELECT 
        amounts_expected.loan_id, 
        amounts_expected.total_amount_expected,
        amounts_paid.total_amount_paid
    FROM amounts_expected
    LEFT JOIN amounts_paid 
        ON amounts_paid.loan_id = amounts_expected.loan_id
),

loan_data AS (

    SELECT 
        loans.loan_id,
        loans.borrower_id,
        borrowers.borrower_credit_score,
        loans.date_of_release,
        loans.term,
        loans.loan_amount,
        loans.down_payment,
        borrowers.state,
        borrowers.city,
        borrowers.zip_code,
        ROUND(loans.payment_frequency,2) AS payment_frequency,
        loans.maturity_date,
        dpd.last_due_date,
        dpd.last_repayment_date,
        dpd.current_days_past_due,
        loan_amounts.total_amount_expected,
        loan_amounts.total_amount_paid,
        final_amt_risk.amount_risk as amount_at_risk,
        final_par_days.par_days
    FROM loans 
    LEFT JOIN borrowers 
        ON borrowers.borrower_id = loans.borrower_id
    LEFT JOIN dpd
        ON dpd.loan_id = loans.loan_id
    LEFT JOIN loan_amounts
        ON loan_amounts.loan_id = loans.loan_id
    LEFT JOIN final_par_days
        ON final_par_days.loan_id = loans.loan_id
    LEFT JOIN final_amt_risk
        ON final_amt_risk.loan_id = loans.loan_id
)


SELECT * FROM loan_data
-- WHERE loan_id = '32u09wekjbfje'
-- ORDER BY payment_num ASC




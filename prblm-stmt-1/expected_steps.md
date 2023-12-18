Expected steps: 
1.	Go through the above schemas, try to understand the relationship between the schemas
See the file names step-1-erd.png for a high-level view of the relationships as outlined. 

2.	Transform the data based on the output as listed in the figure below and save output in this format“output_name.xls”
See the folder named step-2-transform for the requested output file (output_name.xlsx). The file has 18 out of 21 fields outlined in the instructions received. The following fields were missing from any of the tables – branch, branch_id and borrower_name.

Within the same folder, there are 2 additional files, the python script (transform.py) which runs the logic in the SQL file (transform.sql)

3.	Calculate PAR Days - Par Days means the number of days the loan was not paid in full. Eg If the loan repayment was due on the 10th Feb 2022 and payment was made on the 15th Feb 2022 the par days would be 5 days
Implemented as such – 4 scenarios to capture edge cases of lumpsum payment (as CASE WHEN):

SCENARIO 1: first-time payments, calculate PAR days from scheduled date and paid date

For subsequent payments
SCENARIO 2: paid on schedule, and in full, zero PAR days
SCENARIO 3: paid later than scheduled, and in full, and not from a previous payment, calculate PAR days from scheduled date and paid date
SCENARIO 4: paid earlier than scheduled (lumpsum), from a previous payment and covers this month's expected in full, zero PAR days

These 4 scenarios captured all edge cases in the data provided.

4.	Do note for each day a customer missed a payment the amount_at_risk is the total amount of money we are expecting from the customer as at that time, for instance, if the customer owes for 5000 from month 1 and 10000 for current month the amount_at_risk will be the total amount 5000 + 10000 = 15000
The amount owed at any point in time was calculated by taking the running_total_expected minus the running_total_paid. This gave us a point-in-time figure of the amount_owed. To get the final_figure of the amount_at_risk, I took the last amount_owed at the end of the projected schedule. This nets off the total expected so far vs the total received so far.

5.	You can use SQL or Python to achieve this
Both have been used. SQL has the main transform logic, Python only used to “simulate” db with SQLite and run queries

6.	Push this to a github repo alongside the solution for statement 2, detailing your solution.
Done, on my personal gitbuh here is a link to the repository, it is public for visibility - https://github.com/stamilibrahim/autochek-data-engineer

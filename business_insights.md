Business Insights: Digital Payments, 1–31 Aug 2026
Scope: 15,000 transactions worth ₹2.96 crore (₹29.59M). Median ticket ₹1,190 vs mean ₹1,973, so a few large payments pull the average up. The top 10% of transactions (₹4,282 and above) carry 39% of all value.
Key findings
UPI dominates: 65% of transactions and 65% of value (₹1.93 crore). Card is 24% of volume, wallet 10%.
Success rate is 93.3%. 5.1% of transactions fail and 1.7% go to review. UPI succeeds 93.5% of the time, card 92.9%, wallet 92.8%. The gaps are small.
Fraud flags: 2.63% of transactions (395) are flagged, worth ₹8.2 lakh. All 249 REVIEW rows are flagged, plus about 19% of failed ones. Flagged transactions average a risk score of 58.9 vs 35.9 for the rest, so the score separates them well.
High-value risk: transactions above ₹10,000 are only 1.7% of volume but 13.7% of value, and 5.1% of them are flagged, about double the overall rate.
Categories and cities: Fuel leads on value (₹31.2 lakh); Grocery has the highest flag rate (3.42%) and Fuel the lowest (1.68%). Chennai leads cities on value (₹31.7 lakh); Pune has the highest flag rate (3.2%), Kolkata the lowest (1.6%).
Reliability: Bank of Baroda fails most (5.52%), HDFC least (4.37%). Web has the weakest channel outcome (7.4% failed or in review vs 6.3% on POS).
Anomalies
Timing is almost flat across the day. Hourly volume ranges only from 595 to 677, and 2–3 am is as busy as 9 am (the peak, 677). Real retail traffic normally dips at night, so this looks synthetic or worth checking for automated activity.
Weekends and Monday run about 25% heavier (about 2,400 per day vs about 1,900 Tue–Thu).
Response time is ~850 ms for every bank and channel (841–866 ms). Failed payments are no slower than successful ones.
4 rows have an `hour` column that disagrees with `timestamp`; the dashboard uses the timestamp.
Recommendations
Add step-up checks for payments above ₹10,000, such as extra authentication or a velocity limit. This band carries 14% of value and double the flag rate, so it is the best place to spend review effort. Add Grocery, Fashion and Pune as secondary watch lists.
Open a reliability review with Bank of Baroda and the Web channel. Cutting Bank of Baroda's failure rate to the HDFC level would recover roughly 1.2 points of failures on its volume. Offer retry or fallback to UPI on Web.
Plan capacity for Saturday to Monday and keep fraud-review staffing flat across the day rather than night-light, since volume does not fall overnight. Verify the overnight pattern first.
All figures were computed directly from the CSV. The dataset is a sample, so treat differences of under about 1 point as noise.

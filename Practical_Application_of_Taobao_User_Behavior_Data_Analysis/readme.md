# Taobao User Behavior Analysis with MySQL and RFM Segmentation
 
An end-to-end SQL analysis of real e-commerce behavior data from Alibaba's Taobao platform. The project cleans the raw logs, measures how users move from browsing to buying, tests whether the recommendation mechanism is effective, and segments customers with an RFM model to turn the findings into operating recommendations.
 
## Project Goals
 
- Understand how users behave on the platform (page views, favorites, add-to-cart, purchases).
- Measure conversion rates along different behavioral paths.
- Check whether the most-viewed products are also the best-selling ones.
- Segment users with an RFM model and propose a strategy for each segment.
## Dataset
 
- **Source:** [User Behavior Data from Taobao for Recommendation](https://tianchi.aliyun.com/dataset/649) (Alibaba Tianchi).
- **Fields:** `userID`, `itemID`, `categoryID`, `behaviortype`, `timestamp`.
- **Behavior types:** `pv` (page view), `fav` (favorite), `cart` (add to cart), `buy` (purchase).
The raw CSV is not included in this repository. Download it from the link above.
 
## Tools
 
- MySQL (window functions, CTEs, views, `ntile`)
- SQL for all cleaning, transformation and analysis
- Python (matplotlib, ReportLab) for the figures in the written report
## Repository Contents
 
| File | Description |
| --- | --- |
| `DataProcessing.sql` | Full SQL workflow: import, cleaning, feature building, conversion analysis, Top-20 comparison and RFM segmentation |
| [`E-commerce User Behaviour Analysis.pdf`](./E-commerce%20User%20Behaviour%20Analysis.pdf) | Written report with conclusions and recommendations |
| `readme.md` | This file |
 
## Methodology
 
1. **Import:** create the `userbehavior` table and load the CSV.
2. **Cleaning:** detect and remove duplicate records (same user, item and timestamp) with `ROW_NUMBER()`, then check for missing values.
3. **Sampling:** the full table is very large, so the analysis runs on a 500,000-row working table (`usertest`).
4. **Time features:** convert the Unix timestamp into a calendar date and an hour of day.
5. **Behavior aggregation:** build a user-item view (`usertest_p`) that counts page views, favorites, add-to-carts and purchases for each user-item pair.
6. **Conversion analysis:** compare purchase rates across behavioral paths (direct buy, favorite then buy, add-to-cart then buy).
7. **Recommendation check:** rank the Top 20 items by page views and by purchases and compare the two lists.
8. **RFM segmentation:** compute Recency, Frequency and Monetary values per buyer, score each from 1 to 4 with `NTILE(4)`, and classify users into eight segments.
## Key Findings
 
| Metric | Result |
| --- | --- |
| Page view to purchase conversion | **2.27%** |
| Add-to-cart to purchase conversion | **10.93%** |
| Favorite to purchase conversion | **9.03%** |
 
- **Browsing rarely turns into buying.** Users view a lot of products but few purchase, and the most-viewed items differ from the best-selling items. This points to a recommendation mechanism that is not optimized for transactions.
- **Add-to-cart and favorite signal strong intent.** Users who add to cart or favorite convert at roughly four to five times the overall rate, with add-to-cart the strongest.
- **The user base is concentrated in two groups.** *Important Develop* and *Important Win-back* users together account for about 91% of users.
- **Activity peaks in the evening,** between 18:00 and 23:00.
## Recommendations
 
- **Recommendations and traffic:** raise the weight of top-selling categories and bundle best-sellers with high-traffic, low-conversion products.
- **Conversion path:** make add-to-cart and favorite entry points more prominent and reward the action with light incentives such as coupons.
- **Campaign timing:** schedule promotions and launches in the 18:00 to 23:00 window.
- **Segmented CRM:** grow purchase frequency for *Important Develop* users, and run diagnostic surveys plus comeback offers for *Important Win-back* users. Keep *Important Value* users loyal with VIP perks and reactivate *Important Retain* users with push, email or SMS.
## RFM Segments
 
Users are scored on a 1 to 4 scale for each of R, F and M, and the script splits each score at 2.5.
 
| Segment | Recency | Frequency | Monetary |
| --- | --- | --- | --- |
| Important Value | High | High | High |
| Important Develop | High | Low | High |
| Important Retain | Low | High | High |
| Important Win-back | Low | Low | High |
| General Value / Develop / Retain / Win-back | Same R and F pattern as above | | Low |
 
The SQL labels these segments in Chinese (for example 重要价值用户 for Important Value).
 
## How to Run
 
1. Download `UserBehavior.csv` from the Tianchi link above.
2. Open `DataProcessing.sql` in MySQL (with `local_infile` enabled) and change the file path in the `LOAD DATA LOCAL INFILE` statement to your own path.
3. Run the script section by section: import, cleaning, sampling, conversion analysis, Top-20 comparison, then RFM.
## Limitations
 
- **Sampled data:** the analysis uses a 500,000-row subset taken with `LIMIT`, not a random sample of the full dataset, so results may not generalize to all users.
- **Simulated spending:** the dataset has no price field, so the Monetary value is generated with a random number (`CEIL(RAND() * 199)`). The M score is therefore illustrative only, and RFM results that depend on it should be read as a demonstration of the method.
- **Analysis window:** recency is measured against the fixed reference date 2017-12-03.
## Author
 
Lingzhi Guo. Part of the [MySQL Real-life Projects](../) portfolio.

-- input the data 数据导入
-- https://tianchi.aliyun.com/dataset/649

create table userbehavior(
userID int,
itemID int,
categoryID int,
behaviortype text,
timestamp int
);

load data local infile "/Users/ggglingz_hi/Downloads/UserBehavior.csv"
into table userbehavior
fields terminated by ','
lines terminated by '\n';


-- data cleaning 查重并在有重复数据的情况下进行删除

with duplicate_check as (
    select COUNT(*) as dup_count
    from (
        select userID, itemID, timestamp
        from userbehavior
        group by userID, itemID, timestamp
        having COUNT(*) > 1
    ) t
)

delete from userbehavior
where (userID, itemID, timestamp) in (
    select userID, itemID, timestamp
    from (
        select userID, itemID, timestamp,
               row_number() over(
               partition by userID, itemID, timestamp order by timestamp
				) AS rn
        from userbehavior
    ) as dup
    where rn > 1
)
and exists (
    select * from duplicate_check where dup_count > 0
);


-- check the missing value 查看数据的缺失值

select 
count(userID) as userID, count(itemID) as itemID, 
count(categoryID) as categoryID, count(bahaviortype) as bahaviortype, 
count(timestamp) as timestamp
from userbehavior;


-- process the date value 时间数据处理(数据体量过大 新建测试表格)

create table usertest(
	select * from userbehavior 
    limit 500000 
) ;

alter table usertest 
add column date date,
add column time varchar(10);

update usertest
set date = from_unixtime(timestamp, '%Y-%m-%d'),
time = from_unixtime(timestamp, '%H');


-- Count the number of different user behaviors and calculate purchase conversion rate
-- 统计用户不同行为的数目并计算购买转化率


create view usertest_p as
select userID, itemID,
sum(if(bahaviortype = 'pv', 1, 0)) as click,
sum(if(bahaviortype = 'fav', 1, 0)) as favor,
sum(if(bahaviortype = 'buy', 1, 0)) as buy,
sum(if(bahaviortype = 'cart', 1, 0)) as add_to_cart
from usertest
group by userID, itemID;

select
sum(click) as total_clicks,
sum(buy) as total_buy,
sum(case when click>0 and buy>0 and favor=0 and add_to_cart=0 then buy end) as buy_only,
sum(case when click>0 and favor=0 and add_to_cart>0 then add_to_cart end) as add_to_cart,
sum(case when click>0 and favor>0 and add_to_cart=0 then favor end) as favor,
sum(case when click>0 and buy>0 and favor>0 and add_to_cart=0 then buy end) as favor_and_buy,
sum(case when click>0 and buy>0 and favor=0 and add_to_cart>0 then add_to_cart end) as add_to_cart_and_buy,
sum(case when click>0 and favor>0 and add_to_cart>0 then favor + add_to_cart end) as favor_and_cart,
sum(case when click>0 and buy>0 and favor>0 and add_to_cart>0 then favor + buy + add_to_cart end) as all_three,

-- Calculate the conversion rate 计算购买转化率

concat(
	round(
		100 * sum(buy) /
		nullif(sum(click), 0),
	2), '%'
) as total_buy_rate,

concat(
	round(
		100 * sum(case when click>0 and buy>0 and favor=0 and add_to_cart>0 then add_to_cart end) /
		nullif(sum(case when click>0 and favor=0 and add_to_cart>0 then add_to_cart end), 0),
	2), '%'
) as cart_to_buy_rate,

concat(
	round(
		100 * sum(case when click>0 and buy>0 and favor>0 and add_to_cart=0 then buy end) /
		nullif(sum(case when click>0 and favor>0 and add_to_cart=0 then favor end), 0),
	2), '%'
) as favor_to_buy_rate,

concat(
	round(
		100 * sum(case when buy > 0 and favor > 0 and add_to_cart > 0 then 1 end) /
		nullif(sum(case when click>0 and favor>0 and add_to_cart>0 then favor + add_to_cart end), 0),
	2), '%'
) as car_and_favor_to_buy_rate

from usertest_p;


/* The conversion rate from page views to purchase was only 2.27%, 
	while the conversion rate after adding items to the cart reached 10.93%, 
    and the conversion rate after adding items to favorites was 9.03%. 
    This indicates that users spend a significant amount of time browsing products, 
    but very few actually make a purchase. 
    The likely cause is a problem with the platform's recommendation mechanism. 
    The next step will be to filter the top 20 products by clicks and the top 20 by purchase 
    volume, and use the overlap rate to determine the effectiveness of 
    the recommendation mechanism.
	浏览量的购买转化率仅有2.27%，而添加购物车后的购买转化率可以达到10.93%，收藏后购买的转化率达到9.03%。
	用户花费大量时间浏览商品，真正购买的却很少。推测原因为平台的推荐机制有问题。
    下一步将筛选浏览量前20与购买量前20的产品，通过重叠率高低判断推荐机制是否有效 */
    
with
click_rank as (
    select 
        itemID as top_click_item,
        sum(click) as click_amount,
        row_number() over (order by sum(click) desc) as rn
    from usertest_p
    group by itemID
    order by click_amount desc
    limit 20
),
buy_rank as (
    select 
        itemID as top_buy_item,
        sum(buy) as buy_amount,
        row_number() over (order by sum(buy) desc) as rn
    from usertest_p
    group by itemID
    order by buy_amount desc
    limit 20
)
select 
    c.top_click_item,
    c.click_amount,
    b.top_buy_item,
    b.buy_amount
from click_rank c
left join buy_rank b on c.rn = b.rn
order by c.rn;

-- customer analysis based on the RFM analysis model (R=Recency, F=Frequency, M=Monetary)
-- 基于RFM分析模型对用户进行分层分析（消费金额由于数据缺失，将使用随机数生成一列代替）

alter table usertest add price float;
update usertest set price = ceil(rand() * 199);
select * from usertest limit 10;

create view rfm_p as
(select
userID,
datediff('2017-12-03', max(date)) + 1 as recency,
count(userID) as frequency,
sum(price) as monetary
from usertest
where behaviortype = 'buy'
group by userID
order by monetary desc);



-- customer layering using quantile and rating 用分位数法对用户进行分层


create table RFM(
select *, 
ntile(4) over (order by recency desc) as Rscore,   -- 越近访问分越高
ntile(4) over (order by frequency asc) as Fscore,   -- 购买越频繁分越高
ntile(4) over (order by monetary asc) as Mscore    -- 总消费额越高分越高
from rfm_p
);

select 
  classuser, 
  count(userID) as peoplenumber,
  ROUND(count(userID) * 100.0 / total.total_count, 2) as percentage
from
(
  select userID,
    case 
      when Rscore > 2.5 and Fscore > 2.5 and Mscore > 2.5 then '重要价值用户'
      when Rscore > 2.5 and Fscore <= 2.5 and Mscore > 2.5 then '重要发展用户'
      when Rscore <= 2.5 and Fscore > 2.5 and Mscore > 2.5 then '重要保持用户'
      when Rscore <= 2.5 and Fscore <= 2.5 and Mscore > 2.5 then '重要挽留用户'
      when Rscore > 2.5 and Fscore > 2.5 and Mscore < 2.5 then '一般价值用户'
      when Rscore > 2.5 and Fscore <= 2.5 and Mscore <= 2.5 then '一般发展用户'
      when Rscore <= 2.5 and Fscore > 2.5 and Mscore <= 2.5 then '一般保持用户'
      when Rscore <= 2.5 and Fscore <= 2.5 and Mscore <= 2.5 then '一般挽留用户' 
    end as classuser
  from RFM
) as t
cross join
(
  select count(*) as total_count from RFM
) as total
group by classuser, total.total_count
order by percentage desc;

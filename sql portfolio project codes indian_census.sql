select * from [India_census ]..Data1

select * from [India_census ]..Data2

--- Number of rows in our data set

select count(*) from [India_census ]..Data1

select count(*) from [India_census ]..Data2


--Generate data for only 2 particular state

select * from [India_census ]..Data1 where state in ('Jharkhand','Bihar')

--- Population of India

select sum(Population) as total_Population from [India_census ]..Data2

--- converting growth column to float from nvarchar and removing % sign 
SELECT
    CONVERT(float, REPLACE(Growth,'%','')) AS 'Growth'
 FROM [India_census ]..Data1


---Average growth of India
SELECT
AVG(CONVERT(float, REPLACE(Growth,'%',''))) AS 'Growth'
FROM [India_census ]..Data1

---  Avg growth by state 

SELECT
State,AVG(CONVERT(float, REPLACE(Growth,'%',''))) AS 'Growth'
FROM [India_census ]..Data1
group by state

--Avg sex ratio by state 

SELECT
State,AVG(Sex_Ratio) AS 'AVG_SEX_RATIO'
FROM [India_census ]..Data1
group by state
order by AVG_SEX_RATIO desc

-- Avg literacy rate by state

select State,Round(AVG(Literacy),0) AVG_LITERACY_RATE
from [India_census ]..Data1
group by State
order by AVG_LITERACY_RATE desc


-- Avg literacy rate greater then 90

select State,Round(AVG(Literacy),0) AVG_LITERACY_RATE
from [India_census ]..Data1
group by State
having Round(AVG(Literacy),0) >90
order by AVG_LITERACY_RATE desc


---  top 3 state showing highest growth rate 

SELECT
 TOP 3 State,AVG(CONVERT(float, REPLACE(Growth,'%',''))) AS 'Growth'
FROM [India_census ]..Data1
group by state
order by Growth desc

-- Bottom 3 state of growth  

SELECT
top 3 State,AVG(CONVERT(float, REPLACE(Growth,'%',''))) AS 'Growth'
FROM [India_census ]..Data1
group by state
order by Growth 


-- Bottom 3 state of sex ratio

SELECT
 top 3 State,AVG(Sex_Ratio) AS 'AVG_SEX_RATIO'
FROM [India_census ]..Data1
group by state
order by AVG_SEX_RATIO asc


---- top and bottom 3 states in literacy state (BY using temp table and union operator)

drop table if exists #topstates;  ---(creation of temp table)
create table #topstates
( state nvarchar(255),
  topstate float

  )

insert into #topstates   ------------------(inserting  value in temp table)
select state,round(avg(literacy),0) avg_literacy_ratio from [India_census ]..Data1 
group by state order by avg_literacy_ratio desc;

select top 3 * from #topstates order by #topstates.topstate desc;

drop table if exists #bottomstates;    ---(creation of temp table)
create table #bottomstates
( state nvarchar(255),
  bottomstate float

  )

insert into #bottomstates     ------------------(inserting  value in temp table)
select state,round(avg(literacy),0) avg_literacy_ratio from [India_census ]..Data1
group by state order by avg_literacy_ratio desc;

select top 3 * from #bottomstates order by #bottomstates.bottomstate asc;

--union opertor

select * from (
select top 3 * from #topstates order by #topstates.topstate desc) a

union

select * from (
select top 3 * from #bottomstates order by #bottomstates.bottomstate asc) b;


---- states starting with letter a

select distinct state from [India_census ]..Data1 where lower(state) like 'a%' or lower(state) like 'b%'

select distinct state from [India_census ]..Data1 where lower(state) like 'a%' and lower(state) like '%m'


--- joining both tables total Male female population by state

select d.state,sum(d.males) total_males,sum(d.females) total_females from
(select c.district,c.state state,round(c.population/(c.sex_ratio+1),0) males, round((c.population*c.sex_ratio)/(c.sex_ratio+1),0) females from
(select a.district,a.state,round((a.sex_ratio/ 1000),2) sex_ratio,b.population from [India_census ]..Data1 a inner join [India_census ]..Data2 b on a.district=b.district ) c) d
group by d.state


---- total literacy rate by joining table 

select c.state,sum(literate_people) total_literate_pop,sum(illiterate_people) total_lliterate_pop from 
(select d.district,d.state,round(d.literacy_ratio*d.population,0) literate_people,
round((1-d.literacy_ratio)* d.population,0) illiterate_people from
(select a.district,a.state,a.literacy/100 literacy_ratio,b.population from [India_census ]..Data1 a 
inner join [India_census ]..Data2 b on a.district=b.district) d) c
group by c.state


---  population in previous census



select a.district,a.state,CONVERT(float, REPLACE(a.Growth,'%','')) AS growth,b.population from [India_census ]..Data1 a inner join [India_census ]..Data2 b on a.district=b.district


select sum(m.previous_census_population) previous_census_population,sum(m.current_census_population) current_census_population from(
select e.state,sum(e.previous_census_population) previous_census_population,sum(e.current_census_population) current_census_population from
(select d.district,d.state,round(d.population/(1+d.growth),0) previous_census_population,d.population current_census_population from
(select a.district,a.state,CONVERT(float, REPLACE(a.Growth,'%','')) AS growth,b.population from [India_census ]..Data1 a inner join [India_census ]..Data2 b on a.district=b.district) d) e
group by e.state)m


-- Population vs area

select (g.total_area/g.previous_census_population)  as previous_census_population_vs_area, (g.total_area/g.current_census_population) as 
current_census_population_vs_area from
(select q.*,r.total_area from (

select '1' as keyy,n.* from
(select sum(m.previous_census_population) previous_census_population,sum(m.current_census_population) current_census_population from(
select e.state,sum(e.previous_census_population) previous_census_population,sum(e.current_census_population) current_census_population from
(select d.district,d.state,round(d.population/(1+d.growth),0) previous_census_population,d.population current_census_population from
(select a.district,a.state,CONVERT(float, REPLACE(a.Growth,'%','')) AS growth,b.population from [India_census ]..Data1 a inner join [India_census ]..Data2 b on a.district=b.district
) d) e
group by e.state)m) n) q inner join (

select '1' as keyy,z.* from (
select sum(area_km2) total_area from [India_census ]..Data2)z) r on q.keyy=r.keyy)g


--- Window function (output top 3 districts from each state with highest literacy rate)


select a.* from
(select district,state,literacy,rank() over(partition by state order by literacy desc) rnk from [India_census ]..Data1) a

where a.rnk in (1,2,3) 
order by state

--- Window function (output bottom 3 districts from each state with highest literacy rate)


select a.* from
(select district,state,literacy,rank() over(partition by state order by literacy asc) rnk from [India_census ]..Data1) a

where a.rnk in (1,2,3) 
order by state


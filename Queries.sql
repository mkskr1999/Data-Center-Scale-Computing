--How many animals of each type have outcomes? 
SELECT 
  a.animal_type, 
  COUNT(DISTINCT a.animal_id) AS outcome_count
FROM 
  animaldimen AS a
JOIN 
  outcome_fact AS o ON o.animal_dim_key = a.animal_dim_key 
JOIN 
  outcomedimen AS od ON o.outcome_dim_key = od.outcome_dim_key 
WHERE 
  od.outcome_type IS NOT NULL
GROUP BY 
  a.animal_type;

--How many animals are there with more than 1 outcome? 
SELECT 
  COUNT(animal_id) AS animals_with_more_than_one_count 
FROM (
  SELECT 
    a.animal_id 
  FROM 
    animaldimen AS a
  JOIN 
    outcome_fact AS o ON o.animal_dim_key = a.animal_dim_key 
  JOIN 
    outcomedimen AS od ON o.outcome_dim_key = od.outcome_dim_key 
  WHERE 
    od.outcome_type IS NOT NULL
  GROUP BY 
    a.animal_id
  HAVING 
    COUNT(*) > 1
) AS query;

--What are the top 5 months for outcomes? 
SELECT 
  t.mnth,
  COUNT(t.mnth) AS counter 
FROM 
  timingdimenas t
JOIN 
  outcomedimen AS o ON o.time_dim_key = t.time_dim_key 
JOIN 
  outcome_fact AS o2 ON o2.outcome_dim_key = o.outcome_dim_key 
WHERE 
  o.outcome_type IS NOT NULL
GROUP BY 
  t.mnth
ORDER BY 
  counter DESC 
LIMIT 5;

--What is the total number percentage of kittens, adults, and seniors, whose outcome is "Adopted"? 
SELECT
  cat_age_grp,
  COUNT(*) AS total_count,
  ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM outcome_fact WHERE outcome_type = 'Adoption'), 2) AS percentage
FROM 
  outcome_fact AS o
JOIN 
  Cat_Ages cat ON o.animal_dim_key  = cat.animal_dim_key
JOIN 
  outcomedimen AS od ON o.outcome_dim_key  = od.outcome_dim_key 
WHERE 
  od.outcome_type  = 'Adoption'
GROUP BY 
  cat_age_grp;

--Conversely, among all the cats who were "Adopted", what is the total number percentage of kittens, adults, and seniors? 
SELECT
  cat_age_grp,
  COUNT(*) AS total_count,
  ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM outcome_fact WHERE outcome_type = 'Adoption'), 2) AS percentage
FROM 
  outcome_fact AS o
JOIN 
  Cat_Ages cat ON o.animal_dim_key  = cat.animal_dim_key
JOIN 
  outcomedimen AS od ON o.outcome_dim_key  = od.outcome_dim_key 
WHERE 
  od.outcome_type  = 'Adoption' AND cat.animal_type = 'Cat'
GROUP BY 
  cat_age_grp;

--For each date, what is the cumulative total of outcomes up to and including this date? 
SELECT 
  DATE(a.timestmp) AS date_only,
  COUNT(a.animal_dim_key) AS outcomes,
  SUM(COUNT(a.animal_dim_key)) OVER (ORDER BY DATE(a.timestmp)) AS cumulative_total
FROM 
  animaldimen AS a 
LEFT JOIN 
  outcomesfact AS o2 ON a.animal_dim_key = o2.animal_dim_key
LEFT JOIN 
  outcomedimen AS o ON o2.outcome_dim_key = o.outcome_dim_key 
WHERE 
  o.outcome_type IS NOT NULL
GROUP BY 
  DATE(a.timestmp)
ORDER BY 
  date_only;
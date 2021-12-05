select h.page.pagePath, h.page.hostname, count(*) cnt
from `bigquery-public-data.google_analytics_sample.ga_sessions*` ,
unnest (hits) as h
group by h.page.pagePath, h.page.hostname
order by count(*) desc
limit 10;
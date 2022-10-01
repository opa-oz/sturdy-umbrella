select anime_id
from auth_schema.previews as pr
         join auth_schema.anime_list as al
              on pr.anime_id = al.id
where al.status = 'released'
  and pr.last_population_date < current_date - '30 days'::interval
limit 2000

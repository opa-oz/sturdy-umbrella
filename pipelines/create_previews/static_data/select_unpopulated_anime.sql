select anime_id
from auth_schema.previews
where is_populated is False
   or is_populated is Null
limit 1000

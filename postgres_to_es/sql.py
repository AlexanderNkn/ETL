def get_sql_query(index, **kwargs) -> tuple:
    """Returns sql query with variables

    Args:
        index: index in elasticsearch
    Kwargs:
        updated_at: latest update of specified table
        batch_size: transfer batch size

    Returns:
        (sql_query, (value1, value2, ...))

    """
    return {
        'movies': (
            UPDATE_FILMWORK_INDEX,
            (kwargs['updated_at'], kwargs['updated_at'], kwargs['updated_at'], kwargs['batch_size']),
        ),
        'persons': (
            UPDATE_PERSONS_INDEX,
            (kwargs['updated_at'], kwargs['batch_size']),
        ),
        'genres': (
            UPDATE_GENRE_INDEX,
            (kwargs['updated_at'], kwargs['batch_size'])
        ),
    }[index]


# With provided sql all updated data for denormalized film_works info,
# contained persons and genres as well, are selected in one query
UPDATE_FILMWORK_INDEX = """
    SELECT
        fw.id, fw.rating, fw.title, fw.description,
        jsonb_agg(jsonb_build_object('id', p.id, 'full_name', p.full_name, 'role', pfw.role)) AS persons,
        jsonb_agg(jsonb_build_object('id', g.id, 'name', g.name)) AS genres,
        GREATEST(fw.updated_at, MAX(p.updated_at), MAX(g.updated_at)) AS latest_update
    FROM content.film_work fw
    LEFT OUTER JOIN content.person_film_work pfw ON fw.id = pfw.film_work_id
    LEFT OUTER JOIN content.person p ON p.id = pfw.person_id
    LEFT OUTER JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id
    LEFT OUTER JOIN content.genre g ON gfw.genre_id = g.id
    WHERE fw.updated_at > %s or p.updated_at > %s or g.updated_at > %s
    GROUP BY fw.id
    ORDER BY latest_update
    LIMIT %s;
"""

UPDATE_PERSONS_INDEX = """
    SELECT
        p.id,
        p.full_name,
        jsonb_agg(DISTINCT(pfw.role)) as role,
        jsonb_agg(pfw.film_work_id) as film_ids,
        p.updated_at
    FROM content.person p
    INNER JOIN person_film_work pfw
    ON p.id = pfw.person_id
    WHERE p.updated_at > %s
    GROUP BY p.id
    ORDER BY p.updated_at
    LIMIT %s;
"""

UPDATE_GENRE_INDEX = """
    SELECT
        id,
        name,
        description,
        updated_at
    FROM content.genre
    WHERE updated_at > %s
    ORDER BY updated_at
    LIMIT %s;
"""

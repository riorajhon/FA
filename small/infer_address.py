import psycopg2
from psycopg2.extras import RealDictCursor

SQL_INFER_ADDRESS = """
WITH params AS (
  SELECT ST_Transform(
    ST_MakeEnvelope(%s, %s, %s, %s, 4326),
    3857
  ) AS bbox
),

candidate AS (
  SELECT
    osm_id,
    way,
    tags,
    ST_Centroid(way) AS center
  FROM planet_osm_polygon, params
  WHERE building IS NOT NULL
    AND way && params.bbox
    AND ST_Intersects(way, params.bbox)
  ORDER BY ST_Area(way)
  LIMIT 1
),

building_addr AS (
  SELECT tags, center
  FROM candidate
  WHERE tags ? 'addr:housenumber'
),

nearest_addr AS (
  SELECT
    p.tags,
    c.center,
    ST_Distance(
      p.way::geography,
      c.center::geography
    ) AS dist
  FROM planet_osm_point p
  JOIN candidate c ON true
  WHERE p.tags ? 'addr:housenumber'
    AND ST_DWithin(
      p.way::geography,
      c.center::geography,
      50
    )
  ORDER BY dist
  LIMIT 1
),

resolved_addr AS (
  SELECT * FROM building_addr
  UNION ALL
  SELECT tags, center FROM nearest_addr
  LIMIT 1
),

street AS (
  SELECT l.name
  FROM planet_osm_line l
  JOIN candidate c ON true
  WHERE l.highway IS NOT NULL
    AND l.name IS NOT NULL
    AND ST_DWithin(
      l.way::geography,
      c.center::geography,
      50
    )
  ORDER BY ST_Distance(
    l.way::geography,
    c.center::geography
  )
  LIMIT 1
),

city AS (
  SELECT p.name
  FROM planet_osm_polygon p
  JOIN candidate c ON true
  WHERE p.admin_level = '8'
    AND ST_Contains(p.way, c.center)
  LIMIT 1
),

postcode AS (
  SELECT p.tags->'postal_code' AS code
  FROM planet_osm_polygon p
  JOIN candidate c ON true
  WHERE p.boundary = 'postal_code'
    AND ST_Contains(p.way, c.center)
  LIMIT 1
)

SELECT
  r.tags->'addr:housenumber' AS housenumber,
  COALESCE(
    r.tags->'addr:street',
    (SELECT name FROM street)
  ) AS street,
  r.tags->'addr:city' AS city_from_tag,
  (SELECT name FROM city) AS inferred_city,
  COALESCE(
    r.tags->'addr:postcode',
    (SELECT code FROM postcode)
  ) AS postcode,
  ST_X(ST_Transform(r.center, 4326)) AS lon,
  ST_Y(ST_Transform(r.center, 4326)) AS lat
FROM resolved_addr r;
"""

def infer_address(
    conn,
    lon_min,
    lat_min,
    lon_max,
    lat_max
):
    """
    Infer full address context from a tiny bbox.
    Returns dict or None.
    """

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            SQL_INFER_ADDRESS,
            (lon_min, lat_min, lon_max, lat_max)
        )
        row = cur.fetchone()

    return row


if __name__ == "__main__":
    # ---- DB CONNECTION ----
    conn = psycopg2.connect(
        dbname="osm",
        user="postgres",
        password="postgres",
        host="localhost",
        port=5432
    )

    # ---- EXAMPLE tiny bbox (≈10m × 10m, San Francisco) ----
    result = infer_address(
        conn,
        lon_min=-122.41950,
        lat_min=37.77480,
        lon_max=-122.41940,
        lat_max=37.77490
    )

    conn.close()

    if result:
        print("✅ Address found:")
        for k, v in result.items():
            print(f"{k}: {v}")
    else:
        print("❌ No address resolved")

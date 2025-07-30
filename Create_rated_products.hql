USE analyst ;

SELECT
    p.prod_id AS prod_id, p.brand AS brand, p.name AS name,
    p.price AS price, r.rating AS rating, r.message AS msg
  FROM products p
    LEFT OUTER JOIN ratings r
  ON (p.prod_id = r.prod_id)
  WHERE p.prod_id IN (1273641, 1273642, 1273659 )
  ORDER BY prod_id, rating DESC, msg ;

DROP TABLE IF EXISTS rated_products ;

CREATE EXTERNAL TABLE rated_products (
  prod_id INT,
  brand STRING,
  name STRING,
  price INT,
  ratings ARRAY< STRUCT<rating:TINYINT, message:STRING> >
  )
  STORED AS PARQUET
  LOCATION '/analyst/dualcore/rated_products'
  TBLPROPERTIES( 'external.table.purge'='true' ) ;

INSERT OVERWRITE TABLE rated_products
  SELECT
    p.prod_id, p.brand, p.name, p.price,
    collect_list( named_struct('rating', r.rating, 'message', r.message) )
  FROM products p
    LEFT OUTER JOIN ratings r
  ON (p.prod_id = r.prod_id)
  WHERE p.prod_id IN (1273641, 1273642, 1273659 )
  GROUP BY p.prod_id, p.brand, p.name, p.price ;


SELECT prod_id AS prod_id, brand, name, price, ratings
FROM rated_products ;


SELECT prod_id AS prod_id, brand, name, price, rating
FROM rated_products
  LATERAL VIEW explode( ratings ) p AS rating ;


SELECT prod_id AS prod_id, brand, name, price, rating, msg
  FROM rated_products
    LATERAL VIEW inline( ratings ) p AS rating, msg
  ORDER BY prod_id, rating DESC, msg ;

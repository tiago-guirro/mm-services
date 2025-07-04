"""Dados fiscais precificação produto"""
from psycopg import sql

SQL = sql.SQL("""
WITH ufs AS (
  select unnest(ARRAY['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']) as uf_destino
)
SELECT
  rf.aliquota AS icms_origem,
  COALESCE(rf.aliquotaufdestino, 0) + COALESCE(rf.aliquotaicmsfcp, 0) AS icms_destino,
  9.25 - (9.25 * rf.aliquota / 100) AS pis_cofins,
  %(uf_origem)s AS uf_origem,
  ufs.uf_destino
FROM ufs
CROSS JOIN LATERAL (
  SELECT * FROM dsv.ret_regrafiscal_ecode(
    CASE WHEN ufs.uf_destino = %(uf_origem)s THEN 5102 ELSE 6108 END,
    100000000000000,
    CURRENT_DATE,
    10083,
    %(idproduto)s,
    %(uf_origem)s,
    ufs.uf_destino,
    ''
  )
) rf
WHERE 
rf.idtipoimposto = 1""")

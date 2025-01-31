-- TPC_H Query 20 - Potential Part Promotion
SELECT S_NAME, S_ADDRESS
FROM SUPPLIER,
     NATION
WHERE S_SUPPKEY IN (SELECT PS_SUPPKEY
                    FROM PARTSUPP
                    WHERE PS_PARTKEY in (SELECT P_PARTKEY FROM "PART" WHERE P_NAME like 'forest%%')
                      AND PS_AVAILQTY > (SELECT 0.5 * sum(L_QUANTITY)
                                         FROM LINEITEM
                                         WHERE L_PARTKEY = PS_PARTKEY
                                           AND L_SUPPKEY = PS_SUPPKEY
                                           AND L_SHIPDATE >= '1994-01-01'
                                           AND L_SHIPDATE < dateadd(yy, 1, '1994-01-01')))
  AND S_NATIONKEY = N_NATIONKEY
  AND N_NAME = 'CANADA'
ORDER BY S_NAME;
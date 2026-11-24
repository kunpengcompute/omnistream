/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * omnistream is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *         http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 */

CREATE TABLE bid (
    auction  BIGINT,
    bidder   BIGINT,
    price    BIGINT,
    channel VARCHAR,
    url VARCHAR,
    dateTime TIMESTAMP(3),
    extra    VARCHAR
    ) WITH (
    'connector' = 'filesystem',
    'path' = '<path_to_input_genbid_csv>',
    'format' = 'csv'
);

CREATE TABLE auction (
    id BIGINT,
    itemName VARCHAR,
    description VARCHAR,
    initialBid BIGINT,
    reserve BIGINT,
    dateTime TIMESTAMP(3),
    expires TIMESTAMP(3),
    seller BIGINT,
    category BIGINT,
    extra VARCHAR
    ) WITH (
    'connector' = 'filesystem',
    'path' = '<path_to_input_genauction_csv>',
    'format' = 'csv'
);

SET sql-client.execution.result-mode=TABLEAU;

SELECT
    Q.category,
    AVG(Q.final)
FROM (
    SELECT MAX(B.price) AS final, A.category
    FROM auction A, bid B
    WHERE A.id = B.auction AND B.dateTime BETWEEN A.dateTime AND A.expires
    GROUP BY A.id, A.category
) Q
GROUP BY Q.category;

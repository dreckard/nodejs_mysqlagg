-- Start: "%PROGRAMFILES%\MySQL\MySQL Server 5.6\bin\mysqld"
-- Stop: "%PROGRAMFILES%\MySQL\MySQL Server 5.0\bin\mysqladmin" -u root shutdown
CREATE TABLE ad_statistics
(
    ad_id	INT NOT NULL,
    date	DATE NOT NULL,
    impressions	BIGINT NOT NULL,
    clicks	BIGINT NOT NULL,
    spent	BIGINT NOT NULL,
    PRIMARY KEY(ad_id,date)
);

CREATE TABLE ad_actions
(
    ad_id	INT NOT NULL,
    date	DATE NOT NULL,
    action	VARCHAR(128) NOT NULL,
    count	BIGINT NOT NULL,
    value	BIGINT NOT NULL,
    PRIMARY KEY(ad_id,date,action)
    -- FOREIGN KEY(ad_id,date) REFERENCES ad_statistics(ad_id,date)
);

LOAD DATA INFILE 'smartly_data/ad_statistics.tsv' INTO TABLE dbuser.ad_statistics
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';

LOAD DATA INFILE 'smartly_data/ad_actions.tsv' INTO TABLE dbuser.ad_actions
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';

-- This gives you all the data you need to build the JSON
SELECT ads.ad_id,SUM(impressions) impressions,
	   sum(clicks) clicks,
       sum(spent) spent,
       sum(clicks)/sum(impressions) ctr,
       sum(spent)/sum(clicks) cpc,
       1000*sum(spent)/sum(impressions) cpm,
       ada.action,sum(ada.count) count,sum(ada.value) value, sum(spent)/sum(count) cpa
FROM ad_statistics ads
JOIN ad_actions ada
  ON  ads.ad_id = ada.ad_id
  AND ads.date = ada.date
WHERE ads.ad_id IN (?)
AND ads.date BETWEEN ? AND ?
GROUP BY ad_id,ada.action
ORDER BY ad_id,action;

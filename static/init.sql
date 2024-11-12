-- airflow init
CREATE DATABASE airflow_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'airflow_user' IDENTIFIED BY 'airflow_pass';
GRANT ALL PRIVILEGES ON *.* TO 'airflow_user';

-- business db init
USE mining;
CREATE TABLE `hashrate`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `hashrate` bigint NOT NULL,
  `difficulty` bigint NOT NULL,
  `server_ts` timestamp NOT NULL,
  `spider_ts` timestamp NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB;

CREATE TABLE `price`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `USD` int NOT NULL,
  `server_ts` timestamp NOT NULL,
  `spider_ts` timestamp NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB;

CREATE TABLE `avg_info` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `USD` int NOT NULL,
  `hashrate` bigint NOT NULL,
  `difficulty` bigint NOT NULL,
  `spider_ts` timestamp NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB;
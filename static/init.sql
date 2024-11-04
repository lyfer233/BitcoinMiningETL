-- airflow init
CREATE DATABASE airflow_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'airflow_user' IDENTIFIED BY 'airflow_pass';
GRANT ALL PRIVILEGES ON *.* TO 'airflow_user';

-- business db init
USE mining;
CREATE TABLE `hashrate`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `hashrate` varchar(255) NOT NULL,
  `difficulty` varchar(255) NOT NULL,
  `create_time` timestamp NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB;

CREATE TABLE `price`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `time` timestamp NOT NULL,
  `AUD` int NOT NULL,
  `CAD` int NOT NULL,
  `CHF` int NOT NULL,
  `EUR` int NOT NULL,
  `GBP` int NOT NULL,
  `JPY` int NOT NULL,
  `USD` int NOT NULL,
  `create_time` timestamp NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB;

CREATE TABLE `avg_info` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `USD` int NOT NULL,
  `hashrate` varchar(255) NOT NULL,
  `difficulty` varchar(255) NOT NULL,
  `create_time` timestamp NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB;
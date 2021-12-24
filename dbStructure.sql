-- --------------------------------------------------------
-- Host:                         127.0.0.1
-- Server version:               10.6.5-MariaDB - mariadb.org binary distribution
-- Server OS:                    Win64
-- HeidiSQL Version:             11.3.0.6295
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


-- Dumping database structure for stock
CREATE DATABASE IF NOT EXISTS `stock` /*!40100 DEFAULT CHARACTER SET utf8mb3 */;
USE `stock`;

-- Dumping structure for table stock.adjustments
CREATE TABLE IF NOT EXISTS `adjustments` (
  `symbolid` smallint(5) unsigned DEFAULT NULL,
  `date` int(8) unsigned DEFAULT NULL,
  `coefficient` float unsigned DEFAULT NULL,
  `createdAt` timestamp NULL DEFAULT current_timestamp(),
  UNIQUE KEY `symbol-date` (`symbolid`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

-- Data exporting was unselected.

-- Dumping structure for table stock.compare
CREATE TABLE IF NOT EXISTS `compare` (
  `id` varchar(50) DEFAULT NULL,
  `col` varchar(50) DEFAULT NULL,
  `firstval` varchar(50) DEFAULT NULL,
  `secondval` varchar(50) DEFAULT NULL,
  `dev` decimal(20,6) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

-- Data exporting was unselected.

-- Dumping structure for table stock.daily
CREATE TABLE IF NOT EXISTS `daily` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `date` int(8) unsigned DEFAULT NULL,
  `symbolid` smallint(4) unsigned DEFAULT NULL,
  `marketValue` bigint(20) unsigned DEFAULT NULL,
  `vol` bigint(20) unsigned DEFAULT NULL,
  `basicVol` bigint(20) unsigned DEFAULT NULL,
  `fPrice` int(10) unsigned DEFAULT NULL,
  `fPriceDev` float DEFAULT NULL,
  `open` int(10) unsigned DEFAULT NULL,
  `max` int(10) unsigned DEFAULT NULL,
  `min` int(10) unsigned DEFAULT NULL,
  `close` int(10) unsigned DEFAULT NULL,
  `pe` float DEFAULT NULL,
  `realBuyersCount` bigint(20) unsigned DEFAULT NULL,
  `realSellersCount` bigint(20) unsigned DEFAULT NULL,
  `legalBuyersCount` int(10) unsigned DEFAULT NULL,
  `legalSellersCount` int(10) unsigned DEFAULT NULL,
  `realBuyersVol` bigint(20) unsigned DEFAULT NULL,
  `realSellersVol` bigint(20) unsigned DEFAULT NULL,
  `legalBuyersVol` bigint(20) unsigned DEFAULT NULL,
  `legalSellersVol` bigint(20) unsigned DEFAULT NULL,
  `realBuyerPow` bigint(20) DEFAULT NULL,
  `realSellerPow` bigint(20) DEFAULT NULL,
  `realBuyerSellerPowRate` float DEFAULT NULL,
  `legalBuyerSellerPowRate` float DEFAULT NULL,
  `realStakeholdersMoneyFlow` bigint(20) DEFAULT NULL,
  `dailyRealBuyerPowerJump` float DEFAULT NULL,
  `tomorrowRealBuyerSellerPowRate` float DEFAULT NULL,
  `volOnBasicVol` float DEFAULT NULL,
  `candleColor` tinyint(4) DEFAULT NULL,
  `fifteenVolAve` bigint(20) unsigned DEFAULT NULL,
  `threeDaysRealBuyPowerAve` bigint(20) DEFAULT NULL,
  `fiveDaysLaterPrice` int(11) unsigned DEFAULT NULL,
  `fiveDaysLaterPriceDev` float DEFAULT NULL,
  `tenDaysLaterPrice` int(11) unsigned DEFAULT NULL,
  `tenDaysLaterPriceDev` float DEFAULT NULL,
  `twentyDaysLaterPrice` int(11) unsigned DEFAULT NULL,
  `twentyDaysLaterPriceDev` float DEFAULT NULL,
  `thirtyDaysLaterPrice` int(11) unsigned DEFAULT NULL,
  `thirtyDaysLaterPriceDev` float DEFAULT NULL,
  `sixtyDaysLaterPrice` int(11) unsigned DEFAULT NULL,
  `sixtyDaysLaterPriceDev` float DEFAULT NULL,
  `lastDayPrice` int(10) unsigned DEFAULT NULL,
  `tomorrowPrice` int(10) unsigned DEFAULT NULL,
  `volOnFifteenDaysBasicVolAve` float unsigned DEFAULT NULL,
  `threeDaysPriceDevSum` float DEFAULT NULL,
  `adPrice` int(11) unsigned DEFAULT NULL,
  `adVol` bigint(20) DEFAULT NULL,
  `createdAt` timestamp NULL DEFAULT current_timestamp(),
  `updatedAt` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `date-symbol` (`date`,`symbolid`),
  KEY `symbol` (`symbolid`)
) ENGINE=InnoDB AUTO_INCREMENT=176 DEFAULT CHARSET=utf8mb3;

-- Data exporting was unselected.

-- Dumping structure for table stock.errors
CREATE TABLE IF NOT EXISTS `errors` (
  `date` int(11) DEFAULT NULL,
  `symbolid` int(11) DEFAULT NULL,
  `error` text DEFAULT NULL,
  `createdAt` timestamp NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

-- Data exporting was unselected.

-- Dumping structure for table stock.industries
CREATE TABLE IF NOT EXISTS `industries` (
  `id` tinyint(3) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(100) DEFAULT ' ',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=49 DEFAULT CHARSET=utf8mb3;

-- Data exporting was unselected.

-- Dumping structure for table stock.symbols
CREATE TABLE IF NOT EXISTS `symbols` (
  `id` smallint(5) unsigned NOT NULL AUTO_INCREMENT,
  `name` char(50) NOT NULL DEFAULT 'esme namad',
  `co` char(50) DEFAULT 'esme sherkat',
  `industryid` tinyint(4) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  KEY `industry` (`industryid`)
) ENGINE=InnoDB AUTO_INCREMENT=1534 DEFAULT CHARSET=utf8mb3;

-- Data exporting was unselected.

/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;

-- phpMyAdmin SQL Dump
-- version 3.4.10.1
-- http://www.phpmyadmin.net
--
-- 主机: 192.168.0.142:3300
-- 生成日期: 2012 年 08 月 27 日 09:38
-- 服务器版本: 5.1.41
-- PHP 版本: 5.2.13

SET SQL_MODE="NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";
use statty_data;

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- 数据库: `mozat_stat`
--

-- --------------------------------------------------------

--
-- 表的结构 `backend_plan`
--
/*
CREATE TABLE IF NOT EXISTS `backend_plan` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(64) COLLATE utf8_bin NOT NULL,
  `server` varchar(16) COLLATE utf8_bin NOT NULL,
  `repeat_in` int(11) NOT NULL,
  `start` time DEFAULT NULL,
  `heartbeat` int(11) DEFAULT NULL,
  `active` int(1) NOT NULL DEFAULT '0',
  `creation` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=MyISAM  DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=75 ;

-- --------------------------------------------------------

--
-- 表的结构 `backend_plan_alias`
--

CREATE TABLE IF NOT EXISTS `backend_plan_alias` (
  `id` int(11) NOT NULL,
  `plan` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `backend_script`
--

CREATE TABLE IF NOT EXISTS `backend_script` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `plan` int(11) DEFAULT NULL,
  `date` date NOT NULL,
  `name` varchar(256) COLLATE utf8_bin NOT NULL,
  `async` int(1) NOT NULL,
  `active` int(1) NOT NULL DEFAULT '0',
  `remarks` varchar(10240) COLLATE utf8_bin NOT NULL,
  `prev_id` int(11) DEFAULT NULL,
  `seq` int(11) DEFAULT NULL,
  `start_ts` int(11) DEFAULT NULL,
  `end_ts` int(11) DEFAULT NULL,
  `rows` bigint(20) DEFAULT NULL,
  `size` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`date`,`name`,`plan`)
) ENGINE=MyISAM  DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=63660 ;

-- --------------------------------------------------------

--
-- 表的结构 `backend_status`
--

CREATE TABLE IF NOT EXISTS `backend_status` (
  `id` int(11) NOT NULL,
  `script` int(11) NOT NULL,
  `date_str` varchar(32) COLLATE utf8_bin NOT NULL,
  `start_ts` int(11) NOT NULL,
  `last_hb` int(11) NOT NULL,
  `end_ts` int(11) DEFAULT NULL,
  `rows` bigint(20) NOT NULL,
  `size` bigint(20) NOT NULL,
  `lines` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `scipt` (`script`,`start_ts`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------
*/
--
-- 表的结构 `chart`
--

CREATE TABLE IF NOT EXISTS `chart` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `view_id` int(11) NOT NULL,
  `name` varchar(200) NOT NULL,
  `description` text NOT NULL,
  `tab_order` smallint(6) NOT NULL,
  `columns` varchar(1000) NOT NULL COMMENT 'seperated by comma, no quote',
  `column_value_tune` varchar(1000) NOT NULL COMMENT 'comma seperated int values, empty for no need tune',
  `options` varchar(1000) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `view_id` (`view_id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=351 ;

-- --------------------------------------------------------

--
-- 表的结构 `collection`
--

CREATE TABLE IF NOT EXISTS `collection` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `element_count` bigint(11) NOT NULL,
  `element_string_md5` varchar(32) NOT NULL,
  `element_string` longtext NOT NULL,
  PRIMARY KEY (`id`),
  KEY `element_string_md5` (`element_string_md5`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=7337342 ;

-- --------------------------------------------------------

--
-- 表的结构 `data`
--

CREATE TABLE IF NOT EXISTS `data` (
  `key` binary(16) NOT NULL,
  `sub_key` binary(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_int_user_info_globe`
--

CREATE TABLE IF NOT EXISTS `data_int` (
  `key` binary(16) NOT NULL,
  `sub_key` bigint(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


/*
CREATE TABLE IF NOT EXISTS `data_int_user_info_globe` (
  `key` binary(16) NOT NULL,
  `sub_key` bigint(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_int_user_info_mozat`
--

CREATE TABLE IF NOT EXISTS `data_int_user_info_mozat` (
  `key` binary(16) NOT NULL,
  `sub_key` bigint(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_int_user_info_shabik_360`
--

CREATE TABLE IF NOT EXISTS `data_int_user_info_shabik_360` (
  `key` binary(16) NOT NULL,
  `sub_key` bigint(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_int_user_info_stc`
--

CREATE TABLE IF NOT EXISTS `data_int_user_info_stc` (
  `key` binary(16) NOT NULL,
  `sub_key` bigint(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_int_user_info_telk_armor`
--

CREATE TABLE IF NOT EXISTS `data_int_user_info_telk_armor` (
  `key` binary(16) NOT NULL,
  `sub_key` bigint(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_int_user_info_umniah`
--

CREATE TABLE IF NOT EXISTS `data_int_user_info_umniah` (
  `key` binary(16) NOT NULL,
  `sub_key` bigint(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_int_user_info_viva`
--

CREATE TABLE IF NOT EXISTS `data_int_user_info_viva` (
  `key` binary(16) NOT NULL,
  `sub_key` bigint(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_int_user_info_viva_bh`
--

CREATE TABLE IF NOT EXISTS `data_int_user_info_viva_bh` (
  `key` binary(16) NOT NULL,
  `sub_key` bigint(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_int_user_info_vodafone`
--

CREATE TABLE IF NOT EXISTS `data_int_user_info_vodafone` (
  `key` binary(16) NOT NULL,
  `sub_key` bigint(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_url_pattern_globe`
--

CREATE TABLE IF NOT EXISTS `data_url_pattern_globe` (
  `key` binary(16) NOT NULL,
  `sub_key` binary(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_url_pattern_mozat`
--

CREATE TABLE IF NOT EXISTS `data_url_pattern_mozat` (
  `key` binary(16) NOT NULL,
  `sub_key` binary(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_url_pattern_shabik_360`
--

CREATE TABLE IF NOT EXISTS `data_url_pattern_shabik_360` (
  `key` binary(16) NOT NULL,
  `sub_key` binary(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_url_pattern_stc`
--

CREATE TABLE IF NOT EXISTS `data_url_pattern_stc` (
  `key` binary(16) NOT NULL,
  `sub_key` binary(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_url_pattern_telk_armor`
--

CREATE TABLE IF NOT EXISTS `data_url_pattern_telk_armor` (
  `key` binary(16) NOT NULL,
  `sub_key` binary(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_url_pattern_umniah`
--

CREATE TABLE IF NOT EXISTS `data_url_pattern_umniah` (
  `key` binary(16) NOT NULL,
  `sub_key` binary(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_url_pattern_viva`
--

CREATE TABLE IF NOT EXISTS `data_url_pattern_viva` (
  `key` binary(16) NOT NULL,
  `sub_key` binary(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_url_pattern_viva_bh`
--

CREATE TABLE IF NOT EXISTS `data_url_pattern_viva_bh` (
  `key` binary(16) NOT NULL,
  `sub_key` binary(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `data_url_pattern_vodafone`
--

CREATE TABLE IF NOT EXISTS `data_url_pattern_vodafone` (
  `key` binary(16) NOT NULL,
  `sub_key` binary(16) NOT NULL,
  `date` int(10) unsigned NOT NULL,
  `value` double NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `key` (`date`,`sub_key`,`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `edit_view`
--

CREATE TABLE IF NOT EXISTS `edit_view` (
  `id` int(11) DEFAULT NULL,
  `name` varchar(100) NOT NULL,
  `description` blob NOT NULL,
  `sql` blob NOT NULL,
  `script_path` varchar(500) NOT NULL,
  `chart` varchar(1024) NOT NULL COMMENT 'expired',
  `default_tab` int(11) NOT NULL COMMENT 'default chart id, 0 for none',
  `day_range_default` int(11) NOT NULL DEFAULT '0',
  `default_start_date` varchar(20) NOT NULL,
  `include_today` tinyint(4) NOT NULL DEFAULT '0',
  `aggregatable` tinyint(4) NOT NULL DEFAULT '1',
  `created_on` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `conn_string` varchar(500) NOT NULL,
  PRIMARY KEY (`name`),
  KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------
*/
--
-- 表的结构 `group`
--

CREATE TABLE IF NOT EXISTS `group` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL COMMENT 'forced unique',
  `description` text NOT NULL COMMENT 'description',
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='user group' AUTO_INCREMENT=38 ;

-- --------------------------------------------------------

--
-- 表的结构 `group_to_view`
--

CREATE TABLE IF NOT EXISTS `group_to_view` (
  `group_id` int(11) NOT NULL,
  `view_id` int(11) NOT NULL,
  UNIQUE KEY `group_id` (`group_id`,`view_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='relation between view and group';

-- --------------------------------------------------------

--
-- 表的结构 `key_text_dict`
--

CREATE TABLE IF NOT EXISTS `key_text_dict` (
  `md5` binary(16) NOT NULL,
  `text` varchar(8000) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`md5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `log_excution_time_task_hosted`
--

CREATE TABLE IF NOT EXISTS `log_excution_time_task_hosted` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_hosted_id` int(11) NOT NULL,
  `date` varchar(10) NOT NULL,
  `start_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `spent_time_second` float NOT NULL,
  PRIMARY KEY (`id`),
  KEY `task_hosted_id` (`task_hosted_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- 表的结构 `log_log_source`
--

CREATE TABLE IF NOT EXISTS `log_log_source` (
  `id` int(11) NOT NULL,
  `script_file_name` varchar(500) NOT NULL,
  `log_path` varchar(500) NOT NULL,
  `log_path_key` varchar(500) NOT NULL,
  `log_date_time` varchar(20) NOT NULL,
  `process_time_start` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `process_time_end` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `size` bigint(20) NOT NULL,
  `rows` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `script_file_name` (`script_file_name`(191),`log_path_key`(191),`log_date_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- 表的结构 `log_log_source_backup_store`
--

CREATE TABLE IF NOT EXISTS `log_log_source_backup_store` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `log_path` varchar(500) NOT NULL COMMENT 'e.g. \\\\192.168.0.174\\logs_moagent\\internal_perf.log.2011-08-16-10',
  `log_path_key` varchar(500) NOT NULL COMMENT 'e.g. \\\\192.168.0.174\\logs_moagent\\internal_perf.log.',
  `log_date_time` varchar(20) NOT NULL COMMENT '2011-08-16',
  `size` bigint(20) NOT NULL,
  `compressed_size` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `script_file_name` (`log_path_key`(191),`log_date_time`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=43259 ;

-- --------------------------------------------------------

--
-- 表的结构 `log_sql_execution_time`
--

CREATE TABLE IF NOT EXISTS `log_sql_execution_time` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `script_file_name` varchar(500) NOT NULL,
  `sql` text NOT NULL,
  `started_on` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `execution_time_in_sec` float NOT NULL,
  `error_msg` varchar(2048) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `created_on` (`created_on`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=15947439 ;

-- --------------------------------------------------------

--
-- 表的结构 `log_user_access`
--

CREATE TABLE IF NOT EXISTS `log_user_access` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int(10) unsigned NOT NULL,
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `url` varchar(1000) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=349081 ;

-- --------------------------------------------------------

--
-- 表的结构 `notes`
--

CREATE TABLE IF NOT EXISTS `notes` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `mkey` varchar(45) NOT NULL,
  `notes` text NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=154 ;

-- --------------------------------------------------------

--
-- 表的结构 `production_copy_friendship_stc`
--
/*
CREATE TABLE IF NOT EXISTS `production_copy_friendship_stc` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `friend_id` bigint(20) NOT NULL,
  `following` tinyint(1) NOT NULL,
  `followed` tinyint(1) NOT NULL,
  `blocking` tinyint(1) NOT NULL,
  `blocked` tinyint(1) NOT NULL,
  `flags` tinyint(4) NOT NULL,
  `created_on` datetime NOT NULL,
  `modified_on` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `modified_on` (`modified_on`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=97256816 ;

-- --------------------------------------------------------

--
-- 表的结构 `production_copy_friendship_stc_2`
--

CREATE TABLE IF NOT EXISTS `production_copy_friendship_stc_2` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` bigint(20) NOT NULL,
  `friend_id` bigint(20) NOT NULL,
  `following` tinyint(1) NOT NULL,
  `followed` tinyint(1) NOT NULL,
  `blocking` tinyint(1) NOT NULL,
  `blocked` tinyint(1) NOT NULL,
  `flags` tinyint(4) NOT NULL,
  `created_on` datetime NOT NULL,
  `modified_on` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `modified_on` (`modified_on`),
  KEY `created_on` (`created_on`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data`
--
*/
CREATE TABLE IF NOT EXISTS `raw_data` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_ais`
--
/*
CREATE TABLE IF NOT EXISTS `raw_data_ais` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_auto`
--

CREATE TABLE IF NOT EXISTS `raw_data_auto` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_cache_sql_result`
--

CREATE TABLE IF NOT EXISTS `raw_data_cache_sql_result` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_client_crash`
--

CREATE TABLE IF NOT EXISTS `raw_data_client_crash` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_country`
--

CREATE TABLE IF NOT EXISTS `raw_data_country` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_debug`
--

CREATE TABLE IF NOT EXISTS `raw_data_debug` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_device`
--

CREATE TABLE IF NOT EXISTS `raw_data_device` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_device_shabik_360`
--

CREATE TABLE IF NOT EXISTS `raw_data_device_shabik_360` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_globe`
--

CREATE TABLE IF NOT EXISTS `raw_data_globe` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_id_msisdn`
--

CREATE TABLE IF NOT EXISTS `raw_data_id_msisdn` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_ip`
--

CREATE TABLE IF NOT EXISTS `raw_data_ip` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_login_log_mobile`
--

CREATE TABLE IF NOT EXISTS `raw_data_login_log_mobile` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_login_trend`
--

CREATE TABLE IF NOT EXISTS `raw_data_login_trend` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_mchat`
--

CREATE TABLE IF NOT EXISTS `raw_data_mchat` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_monitor`
--

CREATE TABLE IF NOT EXISTS `raw_data_monitor` (
  `oem_name` varchar(100) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_mozat`
--

CREATE TABLE IF NOT EXISTS `raw_data_mozat` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_msisdn`
--

CREATE TABLE IF NOT EXISTS `raw_data_msisdn` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_phone_model`
--

CREATE TABLE IF NOT EXISTS `raw_data_phone_model` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_ranking`
--

CREATE TABLE IF NOT EXISTS `raw_data_ranking` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_sequence_generator`
--

CREATE TABLE IF NOT EXISTS `raw_data_sequence_generator` (
  `oem_name` varchar(30) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `category` varchar(100) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `key` varchar(150) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `date` varchar(20) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_shabik_360`
--

CREATE TABLE IF NOT EXISTS `raw_data_shabik_360` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_sql_dict_result_cache`
--

CREATE TABLE IF NOT EXISTS `raw_data_sql_dict_result_cache` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_stc`
--

CREATE TABLE IF NOT EXISTS `raw_data_stc` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_task_hosted`
--

CREATE TABLE IF NOT EXISTS `raw_data_task_hosted` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_telk_armor`
--

CREATE TABLE IF NOT EXISTS `raw_data_telk_armor` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_test`
--

CREATE TABLE IF NOT EXISTS `raw_data_test` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_trend`
--

CREATE TABLE IF NOT EXISTS `raw_data_trend` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_umniah`
--

CREATE TABLE IF NOT EXISTS `raw_data_umniah` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_umobile`
--

CREATE TABLE IF NOT EXISTS `raw_data_umobile` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_url_pattern`
--

CREATE TABLE IF NOT EXISTS `raw_data_url_pattern` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_url_pattern_globe`
--

CREATE TABLE IF NOT EXISTS `raw_data_url_pattern_globe` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_url_pattern_shabik_360`
--

CREATE TABLE IF NOT EXISTS `raw_data_url_pattern_shabik_360` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_user_access`
--

CREATE TABLE IF NOT EXISTS `raw_data_user_access` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_user_activity_shabik_360`
--

CREATE TABLE IF NOT EXISTS `raw_data_user_activity_shabik_360` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `oem_name` varchar(45) NOT NULL DEFAULT '',
  `category` varchar(1000) NOT NULL,
  `key` varchar(1000) NOT NULL,
  `sub_key` varchar(1000) NOT NULL,
  `date` varchar(20) NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `date` (`date`),
  KEY `oem_name` (`oem_name`,`category`(191),`key`(191),`sub_key`(191)),
  KEY `key` (`key`(191))
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=391690 ;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_user_device`
--

CREATE TABLE IF NOT EXISTS `raw_data_user_device` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_user_device_mozat_6_temp`
--

CREATE TABLE IF NOT EXISTS `raw_data_user_device_mozat_6_temp` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_user_device_type`
--

CREATE TABLE IF NOT EXISTS `raw_data_user_device_type` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_user_info`
--

CREATE TABLE IF NOT EXISTS `raw_data_user_info` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_user_info_dispersion`
--

CREATE TABLE IF NOT EXISTS `raw_data_user_info_dispersion` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_user_info_periodical`
--

CREATE TABLE IF NOT EXISTS `raw_data_user_info_periodical` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_viva`
--

CREATE TABLE IF NOT EXISTS `raw_data_viva` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_viva_bh`
--

CREATE TABLE IF NOT EXISTS `raw_data_viva_bh` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `raw_data_zoota`
--

CREATE TABLE IF NOT EXISTS `raw_data_zoota` (
  `oem_name` varchar(30) COLLATE ascii_bin NOT NULL,
  `category` varchar(100) COLLATE ascii_bin NOT NULL,
  `key` varchar(150) COLLATE ascii_bin NOT NULL,
  `sub_key` varchar(765) COLLATE ascii_bin NOT NULL,
  `date` varchar(20) COLLATE ascii_bin NOT NULL,
  `value` double unsigned NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`oem_name`,`category`,`key`,`sub_key`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------
*/
--
-- 表的结构 `session`
--

CREATE TABLE IF NOT EXISTS `session` (
  `session_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `sesskey` varchar(32) CHARACTER SET latin1 NOT NULL DEFAULT '',
  `expiry` int(11) unsigned NOT NULL DEFAULT '0',
  `creation_time` int(11) unsigned NOT NULL DEFAULT '0',
  `value` varchar(65480) CHARACTER SET latin1 NOT NULL,
  `uid` int(10) unsigned NOT NULL,
  PRIMARY KEY (`session_id`),
  UNIQUE KEY `sesskey` (`sesskey`),
  KEY `uid` (`uid`)
) ENGINE=MyISAM  DEFAULT CHARSET=utf8 AUTO_INCREMENT=11915 ;

-- --------------------------------------------------------

--
-- 表的结构 `sub_key_text_dict`
--

CREATE TABLE IF NOT EXISTS `sub_key_text_dict` (
  `md5` binary(16) NOT NULL,
  `text` varchar(8000) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`md5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- --------------------------------------------------------

--
-- 表的结构 `task_hosted`
--
/*
CREATE TABLE IF NOT EXISTS `task_hosted` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `view_name` varchar(100) NOT NULL,
  `creator_name` varchar(100) NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `type` enum('log_statistic','db_statistic') NOT NULL,
  `is_enabled` tinyint(4) NOT NULL DEFAULT '1',
  `task_definition` text NOT NULL,
  `column_definition` text NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `view_name` (`view_name`),
  KEY `creator_name` (`creator_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------
*/
--
-- 表的结构 `user`
--

CREATE TABLE IF NOT EXISTS `user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `pw` varchar(32) DEFAULT NULL,
  `last_login` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `description` text,
  `can_set_chart` tinyint(1) NOT NULL DEFAULT '0',
  `email` varchar(256) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=130 ;

-- --------------------------------------------------------

--
-- 表的结构 `user_to_group`
--

CREATE TABLE IF NOT EXISTS `user_to_group` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `group_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`,`group_id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=239 ;

-- --------------------------------------------------------

--
-- 表的结构 `value_text_dict`
--

CREATE TABLE IF NOT EXISTS `value_text_dict` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `text` varchar(333) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `text` (`text`)
) ENGINE=MyISAM  DEFAULT CHARSET=utf8 AUTO_INCREMENT=7496 ;

-- --------------------------------------------------------

--
-- 表的结构 `view`
--

CREATE TABLE IF NOT EXISTS `view` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `description` text NOT NULL,
  `sql` longtext NOT NULL,
  `script_path` varchar(500) NOT NULL,
  `chart` varchar(1024) NOT NULL COMMENT 'expired',
  `default_tab` int(11) NOT NULL COMMENT 'default chart id, 0 for none',
  `day_range_default` int(11) NOT NULL DEFAULT '0',
  `default_start_date` varchar(20) NOT NULL,
  `include_today` tinyint(4) NOT NULL DEFAULT '0',
  `aggregatable` tinyint(4) NOT NULL DEFAULT '1',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `conn_string` varchar(500) NOT NULL,
  `backup_info` longtext NOT NULL,
  `charting_javascript` longtext NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 ;

-- --------------------------------------------------------

--
-- 表的结构 `view_backup_2012_06_07`
--
/*
CREATE TABLE IF NOT EXISTS `view_backup_2012_06_07` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `description` text NOT NULL,
  `sql` longtext NOT NULL,
  `script_path` varchar(500) NOT NULL,
  `chart` varchar(1024) NOT NULL COMMENT 'expired',
  `default_tab` int(11) NOT NULL COMMENT 'default chart id, 0 for none',
  `day_range_default` int(11) NOT NULL DEFAULT '0',
  `default_start_date` varchar(20) NOT NULL,
  `include_today` tinyint(4) NOT NULL DEFAULT '0',
  `aggregatable` tinyint(4) NOT NULL DEFAULT '1',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `conn_string` varchar(500) NOT NULL,
  `backup_info` longtext NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=1206 ;
*/
-- --------------------------------------------------------

--
-- 表的结构 `view_monthly`
--

CREATE TABLE IF NOT EXISTS `view_monthly` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `description` text NOT NULL,
  `sql` text NOT NULL,
  `script_path` varchar(500) NOT NULL,
  `chart` varchar(1024) NOT NULL COMMENT 'expired',
  `default_tab` int(11) NOT NULL COMMENT 'default chart id, 0 for none',
  `day_range_default` int(11) NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `conn_string` varchar(500) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=1188 ;

-- --------------------------------------------------------

--
-- 表的结构 `view_seasonly`
--

CREATE TABLE IF NOT EXISTS `view_seasonly` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `description` text NOT NULL,
  `sql` text NOT NULL,
  `script_path` varchar(500) NOT NULL,
  `chart` varchar(1024) NOT NULL COMMENT 'expired',
  `default_tab` int(11) NOT NULL COMMENT 'default chart id, 0 for none',
  `day_range_default` int(11) NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `conn_string` varchar(500) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- 表的结构 `view_weekly`
--

CREATE TABLE IF NOT EXISTS `view_weekly` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `description` text NOT NULL,
  `sql` text NOT NULL,
  `script_path` varchar(500) NOT NULL,
  `chart` varchar(1024) NOT NULL COMMENT 'expired',
  `default_tab` int(11) NOT NULL COMMENT 'default chart id, 0 for none',
  `day_range_default` int(11) NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `conn_string` varchar(500) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=1062 ;

-- --------------------------------------------------------

--
-- 表的结构 `view_yearly`
--

CREATE TABLE IF NOT EXISTS `view_yearly` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `description` text NOT NULL,
  `sql` text NOT NULL,
  `script_path` varchar(500) NOT NULL,
  `chart` varchar(1024) NOT NULL COMMENT 'expired',
  `default_tab` int(11) NOT NULL COMMENT 'default chart id, 0 for none',
  `day_range_default` int(11) NOT NULL DEFAULT '0',
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `conn_string` varchar(500) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

-- --------------------------------------------------------

--
-- 表的结构 `virtual_view`
--

CREATE TABLE IF NOT EXISTS `virtual_view` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL DEFAULT '0',
  `name` varchar(128) NOT NULL,
  `created_on` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `day_range_default` int(10) unsigned NOT NULL,
  `description` text NOT NULL,
  `enabled` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `subscribed` tinyint(3) unsigned NOT NULL DEFAULT '0',
  `options` varchar(1000) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`user_id`,`name`),
  KEY `user_id` (`user_id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=183 ;

-- --------------------------------------------------------

--
-- 表的结构 `virtual_view_cc_userid`
--

CREATE TABLE IF NOT EXISTS `virtual_view_cc_userid` (
  `v_view_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  KEY `v_view_id` (`v_view_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- 表的结构 `virtual_view_chart`
--

CREATE TABLE IF NOT EXISTS `virtual_view_chart` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `view_id` int(11) NOT NULL,
  `name` varchar(200) NOT NULL,
  `description` text NOT NULL,
  `tab_order` smallint(6) NOT NULL,
  `columns` varchar(1000) NOT NULL COMMENT 'seperated by comma, no quote',
  `column_value_tune` varchar(1000) NOT NULL COMMENT 'comma seperated int values, empty for no need tune',
  `options` varchar(1000) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `view_id` (`view_id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=28 ;

-- --------------------------------------------------------

--
-- 表的结构 `virtual_view_item`
--

CREATE TABLE IF NOT EXISTS `virtual_view_item` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `virtual_view_id` int(11) NOT NULL DEFAULT '0',
  `col_name` varchar(128) NOT NULL,
  `alias` varchar(128) NOT NULL,
  `view_id` int(10) unsigned NOT NULL,
  `seq` int(10) unsigned NOT NULL DEFAULT '0',
  `options` text NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `view_id` (`virtual_view_id`,`view_id`,`col_name`,`alias`) USING BTREE
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=1689 ;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;

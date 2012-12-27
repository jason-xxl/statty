<?php
//error_reporting(E_ALL & ~E_NOTICE);

define('DIR_BASE',realpath(dirname(__FILE__).'/..'));
ini_set('include_path', ini_get('include_path')
	.PATH_SEPARATOR.realpath(DIR_BASE.'/lib')
	.PATH_SEPARATOR.realpath(DIR_BASE.'/htdocs'));
require 'util.inc.php';

global $_CONFIG;
$_CONFIG = parse_ini_file(get_conf_file('default.conf'), TRUE);
if ($_CONFIG['ini_set']) foreach ($_CONFIG['ini_set'] as $n => $v)
	ini_set($n, $v);
require 'db.pdo.inc.php';
if ($f = $_CONFIG['basic']['include']) 
	require(get_conf_file($f));
?>

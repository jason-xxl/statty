<?php
header("Content-type: text/html; charset=utf-8");
define('DIR_BASE',realpath(dirname(__FILE__).'/..'));
define('SERVICE_ID',hash('crc32',$_SERVER["HTTP_HOST"]));
ini_set('include_path', ini_get('include_path')
	.PATH_SEPARATOR.realpath(DIR_BASE.'/lib'));
require 'util.inc.php';

global $_CONFIG;
$_CONFIG = parse_ini_file(file_exists($f = get_conf_file(SERVICE_ID.'.conf')) ?
	$f : get_conf_file('default.conf'), TRUE);

if ($_CONFIG['ini_set']) foreach ($_CONFIG['ini_set'] as $n => $v)
	ini_set($n, $v);
define('URL_BASE',$_CONFIG['basic']['url_base']);
require 'db.pdo.inc.php';
if ($f = $_CONFIG['basic']['include']) 
	require(get_conf_file($f));
function on_page_start() {
	if (sess_get_uid())
		db_update('INSERT INTO log_user_access (user_id,url) VALUES (?,?)', array(sess_get_uid(), $GLOBALS['_PAGE']['.']));
}


?>
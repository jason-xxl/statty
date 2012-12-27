<?php
define('SECONDS_PER_HOUR', 3600);
define('SECONDS_PER_DAY', 86400);
define('UINT32_MAX', 4294967295);

//------------------------------------------------------------------------------
// HTTP Request functions
function page_start($session = TRUE) {
	global $_PAGE;
	if ($_PAGE['.'])
		return false;
	$_PAGE['.'] = get_safe_basename($_SERVER['REQUEST_URI']);
	if (function_exists('get_magic_quotes_gpc') && get_magic_quotes_gpc()) {
	    stripslashes_recursive($_GET);
	    stripslashes_recursive($_POST);
	    stripslashes_recursive($_COOKIE);
	}
	if ($_COOKIE['_REQUEST']) {
		parse_str($_COOKIE['_REQUEST'], $_PAGE['_COOKIE']);
	}
	if ($session) {
		session_start();
		$_PAGE['RUNONCE'] = $_SESSION['RUNONCE'];
		unset($_SESSION['RUNONCE']);
	}
	if (function_exists('on_page_start')) {
		on_page_start();
	}
	return true;
}
function redirect($uri, $cookie_request = NULL) {
	if ($cookie_request) {
		setcookie('_REQUEST', $cookie_request, 0, get_absolute_path(
			$_SERVER['REQUEST_URI'], preg_replace('/[\?#].*/','',$uri)));
	}
	if (!$GLOBALS['_PAGE']['.'])
		unset($_SESSION['_REDIRECT_FROM_']);
	else
		$_SESSION['_REDIRECT_FROM_'] = $GLOBALS['_PAGE']['.'];
	header("Location: $uri");
	exit(0);
}
function js_redirect($uri) {
	printf("<script>location.href='%s'</script>", $uri);
	exit(0);
}
function stop($msg) {
	echo $msg;
	exit(0);
}
function is_post() {
	return $_SERVER["REQUEST_METHOD"] == 'POST';
}
function stripslashes_recursive(&$value, $doTrim = TRUE) {
	foreach ($value as $k => $v) {
        if (is_array($v))
            stripslashes_recursive($v);
        else
            $value[$k] = $doTrim ? trim(stripslashes($v)) : stripslashes($v);
    }
}
function get_conf_file($file) {
	return realpath(DIR_BASE.'/conf/'.$file);
}
function get_absolute_path($base, $relative) {
	$base = get_safe_dirname($base);
	$absolutes = $relative[0]=='/'||$base=='/'||$base=='\\' ? array('') : explode('/', $base);
	foreach (explode('/', $relative) as $part) {
		if ('.' == $part || '' == $part) continue;
		if ('..' == $part) {
			array_pop($absolutes);
		} else {
			$absolutes[] = $part;
		}
	}
	return join('/', $absolutes);
}
function get_safe_basename($path) {
	return preg_match('</[-\.\w]+\.php([\?#][^/]*)?$>', $path) ? basename($path) : '.';
}
function get_safe_dirname($path) {
	if ($path == '/' || $path == '\\')
		return '/';
	return preg_match('</[-\.\w]+\.php([\?#][^/]*)?$>', $path) ? dirname($path) : $path;
}
function ie7($str = TRUE) {
	if (preg_match('/[\( ;]MSIE [4-7]\.\d+;/', $_SERVER['HTTP_USER_AGENT']))
		return $str;
	return '';
}
function do_post($url, $data, $optional_headers = null) {
     $params = array('http' => array( 'method' => 'POST', 'content' => $data));
     if ($optional_headers !== null) {
        $params['http']['header'] = $optional_headers;
     }
     $ctx = stream_context_create($params);
     $fp = fopen($url, 'rb', false, $ctx);
     if (!$fp) {
        throw new Exception("Problem with $url, $php_errormsg");
     }
     return stream_get_contents($fp);
}
function do_post_json($url, $data) {
	$headers = "Content-Type: application/json;charset=utf-8\r\n";
	$res = do_post($url, json_encode($data), $headers);
	return is_blank($res) ? NULL : json_decode($res,TRUE);
}
//------------------------------------------------------------------------------
// session functions
function sess_set_uid($uid, $time = NULL) {
	if (is_null($uid))
		unset($GLOBALS['__uid__']);
	else
		$GLOBALS['__uid__'] = $uid;
	if (!is_null($time)) {
		$GLOBALS['__session_creation_time__'] = $time;
	}
}
function sess_get_uid() {
	return $GLOBALS['__uid__'];
}
function _sess_open() {
	connect_db();
	return true;
}
function _sess_close() {
	return true;
}
function _sess_read($id) {
	$sth = db_query("SELECT value,uid,creation_time FROM session WHERE sesskey=?", array($id));
	if ($row = db_fetch_array($sth)) {
		sess_set_uid(intval($row[1]), intval($row[2]));
		return $row[0];
	}
	sess_set_uid(NULL);
	return '';
}
function _sess_write($id, $data) {
	$uid = if_null(sess_get_uid(), 0);
	$sql =  $GLOBALS['__session_creation_time__'] ?
		"UPDATE session SET value=?,uid=?,expiry=UNIX_TIMESTAMP() WHERE sesskey=?" :
		"REPLACE INTO session (value,uid,sesskey,expiry,creation_time) VALUES (?,?,?,UNIX_TIMESTAMP(),UNIX_TIMESTAMP())";
	db_query($sql, array($data,$uid,$id));
	return;
}
function _sess_destroy($id) {
	db_query("DELET FROM session WHERE sesskey=?", array($id));
	return;
}
function _sess_gc($max) {
	if ($max == 0 || !is_time_out($GLOBALS['__session_creation_time__'], $max))
		return 0;
	db_query("DELETE FROM session WHERE expiry<UNIX_TIMESTAMP()-?", array($max));
	return;
}
function _sess_init() {
	session_set_save_handler("_sess_open", "_sess_close", "_sess_read", "_sess_write", "_sess_destroy", "_sess_gc");
	register_shutdown_function('session_write_close');
}

//------------------------------------------------------------------------------
// Data
function init_by_ref(&$ref, $value) {
	if (is_null($ref)) {
		$ref = $value;
	}
	return $ref;
}
function pick() {
    $arg_list = func_get_args();
    if ($arg_list && ($index = $arg_list[0]))
    	return $arg_list[$index];
    return NULL;
}
function is_blank($val) {
    if (is_array($val))
        return count($val) == 0;
    return is_null($val) || is_string($val) && trim($val) === '';
}
function is_assoc($array) { 
    return is_array($array) && array_diff_key($array,array_keys(array_keys($array)));
} 
function nvl($val) {
    if (is_null($val))
        return '';
    return $val;
}
function if_null() {
    $num = func_num_args();
    $arg_list = func_get_args();
    for ($i = 0; $i < $num; $i++) {
        if (!is_null($arg_list[$i]))
            return $arg_list[$i];
    }
    return NULL;
}
function if_blank() {
    $num = func_num_args();
    $arg_list = func_get_args();
    for ($i = 0; $i < $num; $i++) {
        if (!is_blank($arg_list[$i]))
            return $arg_list[$i];
    }
    return NULL;
}
function ors() {
    $num = func_num_args();
    $arg_list = func_get_args();
    for ($i = 0; $i < $num-1; $i++) {
    	if (!empty($arg_list[$i]))
            return $arg_list[$i];
    }
    return $arg_list[$i];
}
function ands() {
    $num = func_num_args();
    $arg_list = func_get_args();
    for ($i = 0; $i < $num-1; $i++) {
    	if (empty($arg_list[$i]))
            return $arg_list[$i];
    }
    return $arg_list[$i];
}
function array_unset_blank($array) {
    foreach ($array as $i => $val) {
        if (is_blank($val))
            unset($array[$i]);
    }
    return $array;
}
function array_modify($a1, $a2) {
	foreach ($a2 as $k => $v)
		$a1[$k] = is_array($v)&&is_array($a1[$k]) ? array_modify($a1[$k],$v) : $v;
	return $a1;
}
function array_get_key($array, $value) {
	foreach ($array as $k => $v)
		if ($v == $value)
			return $k;
	return NULL;
}
function sub_array(&$array, $keys, $vals=array()) {
	$values = array();
	foreach ($keys as $i) {
		$values[] = $array[$i];
	}
	foreach ($vals as $v) {
		$values[] = $v;
	}
	return $values;
}
function walk_to_int(&$array, $list = NULL) {
    foreach ($array as $i => $val) {
        if (is_null($list) || in_array($i, $list))
            $array[$i] = to_int($val);
    }
}
function to_int($val) {
	if (is_int($val))
		return $val;
    if (is_numeric($val))
        return intval($val);
    if (is_array($val)) {
        walk_to_int($val);
        return $val;
    }
    return NULL;
}
function int($val, $default = 0) {
	return if_null(to_int($val), $default);
}
function sign($int) {
	if ($int == 0)
		return 0;
	return $int > 0 ? 1 : -1;
}
function truncate($o1, $o2) {
	return $o1 - ($o1 % $o2);
}
function nstr($format, $value) {
	return !$value ? '' :
		sprintf($format, $value);
}
function substring_after($str, $separator) {
	if (is_null($str) or $str === '')
		return $str;
	if (is_null($separator) || ($i = stripos($str, $separator)) === FALSE)
		return '';
	return substr($str, $i+1);
}
function substring_before($str, $separator) {
	if (is_null($str) or $str === '')
		return $str;
	if ($separator === '')
		return '';
	if (is_null($separator) || ($i = stripos($str, $separator)) === FALSE)
		return $str;
	return substr($str, 0, $i);
}
function ends_with($str,$str2) {
	return !is_blank($str2) && strstr($str,$str2) == $str2;
}
function starts_with($str,$str2) {
	return !is_blank($str2) && substr($str,0,strlen($str2)) == $str2;
}
function now() {
	return date('Y-m-d H:i:s');
}
function is_time_out($ts, $to) {
	if (!is_int($ts))
		$ts = strtotime($ts);
	return time() - $ts > $to;
}
function truncate_date($ts) {
	return intval(strtotime('today',$ts));
}
function validate_patterns(&$arr, $patterns) {
	foreach ($patterns as $n => $p) {
		if (!is_blank($arr[$n]) && ($p[0]=='!' xor !preg_match(ltrim($p,'!'), $arr[$n])))
			return FALSE;
	}
	return TRUE;
}
function validate_required(&$arr, $list) {
	foreach ($list as $k) {
		if (is_blank($arr[$k]))
			return FALSE;
	}
	return TRUE;
}
function validate_email($email) {
	return eregi("^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,3})$", $email);
}
function generate_password($n = 8) {
	$code = '';
	srand((double)microtime()*1000000);
	$characters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890';
	$m = strlen($characters);
	for ($i = 0; $i < $n; $i++) {
		$code .= substr($characters,rand()%$m,1);
  	}
  	return $code;
}
function sql_array($arr) {
	return join(',',array_fill(0, count($arr), '?'));
}
function debug_dump() {
	return '<pre>'.print_r($_REQUEST, true).'</pre>';
}
function truncate_range($num, $min, $max) {
	return min(max($num, $min), $max);
}
function file_size_kb($num) {
	if ($num <= 0)
		return '0 KB';
	$d = truncate_range((int)(4.35-log($num)/3.4),0,3);
	return number_format($num/1024, $d).' KB';
	
}
//------------------------------------------------------------------------------
// Error & Logger
function error_handler($errno, $errmsg, $filename, $linenum, $vars) {
	if ($errno & E_USER_ERROR)
		error_log($errmsg);
	elseif ($errno & E_ALL & ~E_NOTICE)
		error_log("$errmsg ($filename:$linenum)");
}
function exception_handler($exception) {
	error_log($exception->getMessage()."\n".$exception->getTraceAsString());
}
function _error_handler_init($logfile = NULL) {
	set_error_handler("error_handler");
	set_exception_handler('exception_handler');
}
//------------------------------------------------------------------------------
// HTML functions
function q($str) {
    if (is_blank($str))
        return '';
    return htmlspecialchars($str);
}
function d($str) {
    if (is_blank($str))
        return '';
	if (strlen($str)==8 && substr($str,0,3)=='201'){
		return substr($str,0,4).'-'.substr($str,4,2).'-'.substr($str,6,2);
	}
    return htmlspecialchars($str);
}
function h($str, $show) {
	return $show ? " $str=\"$str\"" : '';
}
function n($num) {
	if (is_string($num))
		$num = str_replace(',','',$num);
	if (is_blank($num))
		return '-';
	if (strpos($num,' ')!==FALSE) //can use ' ' to force show raw number
		return trim($num);
	if (is_numeric($num)) 
		return  strstr("$num",'.') ? number_format($num, 2) : number_format($num);
	return $num;
}
function l($str, $subs = NULL) {
	global $_CONFIG;
	$r = ors($_CONFIG['LC_STR'][$str], $_CONFIG['LC_STR_EN'][$str], $str);
	if (is_array($r) && !is_null($subs))
		return $r[$subs];
	return $r;
}
function s($str) {
	return addcslashes($str,'"\\');
}
function e($v) {
	return preg_replace("#(.+)#e","\$1",$v);
}
function pl($img) {
	$GLOBALS['_PAGE']['PRELOAD'][] = $img;
	return $img;
}
function set_download_header($mime, $name, $date=NULL) {
    if (isset($_SERVER["HTTPS"])) {
        header("Pragma: ");
        header("Cache-Control: ");
        header("Cache-Control: no-store, no-cache, must-revalidate"); // HTTP/1.1
        header("Cache-Control: post-check=0, pre-check=0", FALSE);
    }
    else {
        header("Cache-control: private");
    }
    header("Content-Type: $mime");
    header("Content-Disposition: attachment; filename=\"".trim(htmlentities($name))."\"");
    header("Content-Description: ".trim(htmlentities($name)));
    header("Connection: close");
}
function get_ajax_msgs($messages) {
	$s = '';
	if (is_array($messages))
		foreach ($messages as $message)
			if (is_array($message))
				$s .= sprintf('<span class="ajax_msg_%s">%s</span>', $message['type'], q($message['text']));
	return $s;
}
define('LIB_PCHART', 'pchart.inc.php');
?>
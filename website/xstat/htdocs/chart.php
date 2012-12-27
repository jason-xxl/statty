<?php
include("common.inc.php");
require_once(LIB_PCHART);

if ($_GET['h'])
	pchart_output($_GET['h']);
else
	header('HTTP/1.0 404 Not Found');
?>
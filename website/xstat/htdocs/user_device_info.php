<?php
set_time_limit(180);
require('common.inc.php');
page_start();

$user_agent = db_fetch_assoc(db_query('SELECT `value_text_dict`.text as text_value FROM `raw_data_user_info` left join `value_text_dict` on `raw_data_user_info`.`value`=`value_text_dict`.`id` WHERE `oem_name`=? and `category`="moagent" and `key`="from_app_request_by_monet_id_user_agent_first_text_value" and `sub_key`=?',array($_GET['oem'],$_GET['id'])));

$client_width = db_fetch_assoc(db_query('SELECT * FROM `raw_data_user_info` WHERE `oem_name`=? and `category`="moagent" and `key`="from_app_request_by_monet_id_client_width_first_int_value" and `sub_key`=?',array($_GET['oem'],$_GET['id'])));

$client_height = db_fetch_assoc(db_query('SELECT * FROM `raw_data_user_info` WHERE `oem_name`=? and `category`="moagent" and `key`="from_app_request_by_monet_id_client_height_first_int_value" and `sub_key`=?',array($_GET['oem'],$_GET['id'])));

$client_ip = db_fetch_assoc(db_query('SELECT * FROM `raw_data_user_info` WHERE `oem_name`=? and `category`="moagent" and `key`="from_app_request_by_monet_id_ip_first_int_value" and `sub_key`=?',array($_GET['oem'],$_GET['id'])));

?>
<html>
<head>
<style type="text/css">
	table { border: 1px solid gray; }
</style>
<link href="./css/jquery-ui.css" rel="stylesheet" type="text/css"/>

<script type="text/javascript" src="./js/jquery.js"></script>
<script type="text/javascript" src="./js/jquery.ui.js"></script>
<script type="text/javascript" charset="utf-8">
	$(document).ready(function() {
		
	});
</script>
</head>
<body>
<b>User Device Info</b>
<p></p>

<TABLE border=0>

<TR>
	<TD width="20%">Monet Id:</TD>
	<TD><?=q($_GET['id']) ?></TD>
</TR>

<TR>
	<TD>User Agent:</TD>
	<TD><?=q($user_agent['text_value']) ?></TD>
</TR>

<TR>
	<TD>Screen Width:</TD>
	<TD><?=q($client_width['value']) ?></TD>
</TR>

<TR>
	<TD>Screen Height:</TD>
	<TD><?=q($client_height['value']) ?></TD>
</TR>

<TR>
	<TD>Last IP:</TD>
	<TD><?=q(long2ip($client_ip['value'])) ?></TD>
</TR>

</TABLE>


</body>
</html>
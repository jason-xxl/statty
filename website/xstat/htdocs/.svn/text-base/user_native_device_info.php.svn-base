<?php
set_time_limit(180);
require('common.inc.php');
page_start();
//from raw_data_user_info_periodical where `oem_name`="STC" and `category`="website" and `key`="from_download_link_by_date_by_phone_number_native_user_agent_first_text_value" and `sub_key`="966551113096"

$msisdn=explode(',',$_GET['msisdn']);


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

<?php

foreach($msisdn as $i){

$user_agent = db_fetch_assoc(db_query('SELECT `value_text_dict`.text as text_value FROM `raw_data_user_info_periodical` left join `value_text_dict` on `raw_data_user_info_periodical`.`value`=`value_text_dict`.`id` WHERE `oem_name`=? and `category`="website" and `key`="from_download_link_by_date_by_phone_number_native_user_agent_first_text_value" and `sub_key` in (?)',array($_GET['oem'],$i)));


?>

<hr/>
<TABLE style="border:0px;">

<TR>
	<TD width="20%">Msisdn:</TD>
	<TD><?=q($i) ?></TD>
</TR>

<TR>
	<TD>Native User Agent:</TD>
	<TD><?=q($user_agent['text_value']) ?></TD>
</TR>


</TABLE>

<?php
	
}

?>
</body>
</html>
<?php
set_time_limit(180);
require('common.inc.php');
page_start();
if (!sess_get_uid())
	redirect('login.php');
require('xstat.inc.php');
?>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN">

<HTML>

<HEAD>
    <TITLE>Stat Portal</TITLE>
    <META NAME="Generator" CONTENT="EditPlus">
    <META NAME="Author" CONTENT="">
    <META NAME="Keywords" CONTENT="">
    <META NAME="Description" CONTENT="">
</HEAD>

<frameset cols="210px,*" border="1">
    <frame name="menu" src="user_management_menu.php">
    <frame name="content" src="group.php">
</frameset>

</HTML>

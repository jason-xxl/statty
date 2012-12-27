<?php
require('common.inc.php');

switch($_GET['type']){

	case 1:
		header('Content-Type: image/png');
		$url = sprintf('http://angel.morange.com/rrd/img/1/?&start=%d&end=s%%2b1d&height=200&width=450&',strtotime('yesterday'));
		readfile($url);
		break;

	case 'stc_online':
		header('Content-Type: image/png');
		$url = sprintf('http://angel.morange.com/rrd/img/1/?&start=%d&end=s%%2b1d&height=200&width=450&',strtotime('yesterday'));
		readfile($url);
		break;

	case 'umniah_online':
		header('Content-Type: image/png');
		$url = sprintf('http://angel.morange.com/rrd/img/4/?&start=%d&end=s%%2b1d&height=200&width=450&',strtotime('yesterday'));
		readfile($url);
		break;

	case 'telk_armor_online':
		header('Content-Type: image/png');
		$url = sprintf('http://angel.morange.com/rrd/img/6/?&start=%d&end=s%%2b1d&height=200&width=450&',strtotime('yesterday'));
		readfile($url);
		break;

	case 'viva_online':
		header('Content-Type: image/png');
		$url = sprintf('http://angel.morange.com/rrd/img/2/?&start=%d&end=s%%2b1d&height=200&width=450&',strtotime('yesterday'));
		readfile($url);
		break;

	case 'viva_bh_online':
		header('Content-Type: image/png');
		$url = sprintf('http://angel.morange.com/rrd/img/3/?&start=%d&end=s%%2b1d&height=200&width=450&',strtotime('yesterday'));
		readfile($url);
		break;

}
?>
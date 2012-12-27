<?php
function pchart_init() {
	include_once('pChart/pChart.class');
	$ColorMap = array('S1'=>'#527C94','S2'=>'#E0642E','S3'=>'#E0D62E','S4'=>'#2E97E0','S5'=>'#B02EE0','S6'=>'#E02E75', 'S7'=>'#5CE02E', 'S8'=>'#E0B02E', 'S9'=>'#15428B', 'S10'=>'#FFC0CB');
	if (function_exists('posix_getpwuid'))
		$u = @posix_getpwuid(posix_geteuid());
	define('PCACHE_PATH', sys_get_temp_dir().($u ? '/pchart-'.$u['name'] : '/pchart'));
	if (!file_exists(PCACHE_PATH))
		mkdir(PCACHE_PATH, 0777, TRUE);
}
function pchart_hash($ID,$pChart) {
	$mKey = "$ID";
	$tKey = "";
	foreach($pChart as $k => $v) {
		if ($k != 'Data')
			$tKey .= '\t'.$k.'\t'.(is_array($v) ? serialize($v) : "$v");
	}
	$mKey .= md5($tKey);
	foreach($pChart['Data'] as $key => $Values) {
		$tKey = "";
		foreach($Values as $Serie => $Value)
	        $tKey .= $Serie.$Value;
		$mKey .= md5($tKey);
	}
	return(md5($mKey));
}
function pchart_new($ID, $pChart, $NotReadSize = FALSE) {
	$GLOBALS['pChart'] = $pChart;
	$hash = pchart_hash($ID,$pChart);
	if (!file_exists($f = PCACHE_PATH.'/'.$hash)) {
		require("pChart/templates/$ID.inc.php");
		$Chart =& $GLOBALS['pChart']['Chart'];
	    imagepng($Chart->Picture,$f);
	}
    if (!$NotReadSize)
    	$GLOBALS['pChart']['ImgSize'] = getimagesize($f);
    return $hash;
}
function pchart_output($hash) {
	if ( file_exists($f = PCACHE_PATH.'/'.$hash) ) {
       header('Content-type: image/png');
       @readfile($f);
       exit();
    }
}

function get_legends($keys_map, $cols) {
	$val = array();
	foreach ($cols as $v) {
		$k = substring_before($v,'#');
		if (!is_null($keys_map[$k]))
			$val[ors(substring_after($v,'#'),$v)] = $keys_map[$k];
	}
	return $val;
}

function aggr_sum(&$values) {
	foreach ($values as $i => $v) {
		if ($i == 0) {
			$aggr = array_fill(0, count($v), 0);
			$max = $min = $v[0];
		} else {
			$max = max($max, $v[0]);
			$min = min($min, $v[0]);
		}
		foreach ($v as $j => $k)
			$aggr[$j] += $k;
	}
	$aggr[0] = $max > $min ? sprintf('%s ~ %s',$min,$max) : $min;
	return array($aggr);
}

function to_2d_chart_data(&$values, $type, $legends, $options = array()) {
	$chart = array(
		'Data' => array(),
		'DataDescription' => array(
			'Position' => 'Name',
			'Format' => array('Y'=>'metric'),
			'Values' => array(),
			'Description' => array()
		),
		'Options' => array('scaleStart0'=>1),
	);
	$chart = array_modify($chart, $options);
	foreach (array_keys($legends) as $i => $name) {
		$chart['DataDescription']['Values'][] = 'S'.$i;
		$chart['DataDescription']['Description']['S'.$i] = $name;
	}
	foreach (array_reverse(array_keys($values)) as $i) {
		$vs = array('Name'=>$values[$i][0]);
		foreach (array_values($legends) as $j => $k)
			if ($values[$i][$k])
				$vs['S'.$j] = preg_replace('/,|%$/','', $values[$i][$k]);
		$chart['Data'][] = $vs;
	}
	return pchart_new($type,$chart);
}
function to_1d_chart_data(&$values, $type, $legends, $options = array()) {
	$chart = array(
		'Data' => array(),
		'DataDescription' => array(
			'Position' => 'Name',
			'Format' => array('Y'=>'metric'),
			'Values' => array('S1'),
			'Description' => array()
		),
		'Options' => array('scaleStart0'=>1),
	);
	$chart = array_modify($chart, $options);
	foreach ($legends as $name => $i) {
		$chart['Data'][] = array(
			'Name' => $name,
			'S1' => preg_replace('/,|%$/', '', $values[$i]),
		);
	}
	return pchart_new($type,$chart);
}
function drawLegend($Chart,$XPos,$YPos,&$DataDescription,$R,$G,$B) {
	$C_TextColor = imagecolorallocate($Chart->Picture,0,0,0);

	/* <-10->[8]<-4->Text<-10-> */
	$MaxWidth = 0; $MaxHeight = 0;
	foreach($DataDescription["Description"] as $Key => $Value) {
		$Position  = imageftbbox($Chart->FontSize,0,$Chart->FontName,$Value);
		$TextWidth = $Position[2]-$Position[0];
		if ( $Chart->FontSize > $MaxHeight ) { $MaxHeight = $Chart->FontSize; }
		$MaxWidth = $MaxWidth + ( $TextWidth + 22 );
	}
	$MaxHeight = $MaxHeight + 9;
	$MaxWidth  = $MaxWidth + 10;

	$Chart->drawFilledRoundedRectangle($XPos+1,$YPos+1,$XPos+$MaxWidth+1,$YPos+$MaxHeight+1,5,$R-30,$G-30,$B-30);
	$Chart->drawFilledRoundedRectangle($XPos,$YPos,$XPos+$MaxWidth,$YPos+$MaxHeight,5,$R,$G,$B);

	$YOffset = 4 + $Chart->FontSize; $ID = 0;
	$XOffset = 0;
	foreach($DataDescription["Description"] as $Key => $Value) {
		$Chart->drawFilledRoundedRectangle($XPos+$XOffset+10,$YPos+$YOffset-4,$XPos+$XOffset+14,$YPos+$YOffset-4,2,$Chart->Palette[$ID]["R"],$Chart->Palette[$ID]["G"],$Chart->Palette[$ID]["B"]);

		$Position  = imagettftext($Chart->Picture,$Chart->FontSize,0,$XPos+$XOffset+22,$YPos+$YOffset,$C_TextColor,$Chart->FontName,$Value);
		$TextWidth = $Position[2]-$Position[0];
		$XOffset = $XOffset + $TextWidth + 22;
		$ID++;
	}
}
function skipLables($count, $size) {
	return max((int)(130*$count/$size),1);
}
pchart_init();
?>
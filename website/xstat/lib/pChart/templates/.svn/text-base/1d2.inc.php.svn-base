<?php
require_once(LIB_PCHART);

global $pChart;
$Data = $pChart['Data'];
$DataDescription = $pChart['DataDescription'];
$Size = split('x',if_blank($pChart['Size'], '375x200'));
$Font = if_blank($pChart['Font'],6.5);

$height = max($Size[1],181);
$pChart['Chart'] = $Chart = new pChart($Size[0],$height);
$Chart->setColorPalette(8,82,124,148);
$Chart->setColorPalette(9,189,189,189);
$Chart->drawFilledRoundedRectangle(7,7,$Size[0]-7,$height-7,5,240,240,240);   
$Chart->drawRoundedRectangle(5,5,$Size[0]-5,$height-5,5,230,230,230); 

// Draw the pie chart
$t = 0;
if ($Data) foreach ($Data as $d) {
	$t += $d['S1'];
}
if ($t) {
	$Chart->setFontProperties("Fonts/ARIALUNI.ttf",$Font);   
	$Chart->drawPieGraph($Data,$DataDescription,$Size[0]*.333+3,$height/2,$height/2-10,PIE_PERCENTAGE,TRUE,50,20,5);
	$Chart->drawPieLegend($Size[0]*.667,15,$Data,$DataDescription,250,250,250);
}
?>

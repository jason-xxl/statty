<?php
require_once(LIB_PCHART);

global $pChart;
$Data =& $pChart['Data'];
$DataDescription = $pChart['DataDescription'];
$Size = split('x',if_blank($pChart['Size'], '1024x373'));
$Font = if_blank($pChart['Font'],9);
$ScaleMode = $pChart['Options']['scaleStart0']?SCALE_START0:SCALE_NORMAL;

$dw = (int)(35/1024*$Size[0]);
$dh = count($DataDescription["Values"]) > 1 ? 20 : 0;
$skip = skipLables(count($Data),$Size[0]); 
$height = max($Size[1],181);
// Initialise the graph
$pChart['Chart'] = $Chart = new pChart($Size[0],$height);
$Chart->setColorPalette(0,224,100,46);
$Chart->setColorPalette(1,188,224,46);
$Chart->drawFilledRoundedRectangle(7,7,$Size[0]-7,$height-7,5,240,240,240);
$Chart->drawRoundedRectangle(5,5,$Size[0]-5,$height-5,5,230,230,230);
$Chart->setGraphArea(35+$dw,20+$dh,$Size[0]-35-$dw,$height-35);
$Chart->setFontProperties("Fonts/ARIALUNI.ttf",$Font);
if ($dh)
	$Chart->drawLegend2(35+$dw,10,$DataDescription,255,255,255);
$Chart->drawGraphArea(255,255,255,TRUE);
$Chart->drawScale($Data,$DataDescription,$ScaleMode,150,150,150,TRUE,0,0,FALSE,$skip);
$Chart->drawGrid(4,TRUE,230,230,230,50);

// Draw the line graph
$Chart->drawLineGraph($Data,$DataDescription);
?>

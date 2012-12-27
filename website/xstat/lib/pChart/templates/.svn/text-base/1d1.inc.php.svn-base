<?php
require_once(LIB_PCHART);

global $pChart;
$Data = $pChart['Data'];
$DataDescription = $pChart['DataDescription'];
$Size = split('x',if_blank($pChart['Size'], '375x200'));
$Font = if_blank($pChart['Font'],8);
$ScaleMode = $pChart['Options']['scaleStart0']?SCALE_START0:SCALE_NORMAL;
$marginTop = 15;
$marginBottom = 10;

$height = max($Size[1],181);
// Initialise the graph
$pChart['Chart'] = $Chart = new pChart($Size[0],$height);
$Chart->setColorPalette(8,21,66,139); #15428B
$Chart->setColorPalette(9,255,192,203);
$Chart->drawFilledRoundedRectangle(7,7,$Size[0]-7,$height-7,5,240,240,240);
$Chart->drawRoundedRectangle(5,5,$Size[0]-5,$height-5,5,230,230,230);
$Chart->setGraphArea(45,$marginTop,$Size[0]-15,$height-20-$marginBottom);
$Chart->setFontProperties("Fonts/ARIALUNI.ttf",$Font);
$Chart->drawGraphArea(255,255,255,TRUE);
$Chart->drawScale($Data,$DataDescription,$ScaleMode,150,150,150,TRUE,0,0,TRUE);
$Chart->drawGrid(4,TRUE,230,230,230,50);

// Draw the bar graph
$Chart->drawBarGraph($Data,$DataDescription,TRUE);
if (!is_null($pChart['Options']['drawTreshold'])) {
	$Chart->drawTreshold($pChart['Options']['drawTreshold'],51,51,51);
}
?>
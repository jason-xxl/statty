<html>
	<head>
		<title>Statistics Portal</title>
		<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
		<meta http-equiv="X-UA-Compatible" content="IE=EmulateIE7" />
		<script src="./js/jquery.js" type="text/javascript"></script>
		<script src="./js/jquery.center.js" type="text/javascript"></script>
        <link rel="stylesheet" href="./common.css" />
		<style>
		#center_div a {
			color:black;
			font-size:12px;
			text-decoration: none;
		}
		#center_div div {
			text-align: center;
			float:left; 
			width:120px;
		}
		#center_div img {
			border:0;
		}
		#center_div table {
			width:100%;
			text-align:center;
		}
		#center_div td {
			width:25%;
			text-align:center;
		}
		#bottom_div {
			width:400px; 
			height:20px;
			text-align:center; 
			color:gray; 
			font-size:10px;
			position:absolute;
		}
		</style>
	</head>

	<body style="height:80%;">

	<div style="float:right;">
		<button type="button" class="img_btn" style=" font-size:10px;" onclick="location.href='logout.php'"> Logout</button>&nbsp;
	</div>

	<div id="center_div" style="width:400px;">

		<table>
			<tr height="95px;">
				<td><a href="browse.php"><img src="images/browse.png" /></a></td>
				<td><a href="user_view.php#tabs-STC"><img src="images/all_views.png" style=" width:74px;height:66px; margin-left:3px; margin-top:4px;" /></a></td>
				<td><a href="analyz/"><img src="images/analyse.png" style=" width:70px;height:72px; margin-left:-3px; margin-top:2px;" /></a></td>
				<td><a href="user_view_v.php"><img src="images/email.png" style=" margin-top:4px; margin-left:-5px;" /></a></td>
			</tr>
			<tr>
				<td><a href="browse.php">Browse</a></td>
				<td><a href="user_view.php">All Views</a></td>
				<td><a href="analyz/">Analatics</a></td>
				<td><a href="user_view_v.php">My Views</a></td>
			</tr>
		</table>
	
	</div>
	
	<div id="bottom_div">Mozat.com 2010</div>

	<script>

	function centerBottom(selector) {
		var newTop =   $(window).height() - $(selector).height()-30;
		var newLeft = ($(window).width()  - $(selector).width()) / 2;
		$(selector).css({
			'position': 'absolute',
			'left': newLeft,
			'top': newTop
		});
	}

	$(function(){
		$('#center_div').center();
		centerBottom("#bottom_div");

	});

	$(window).resize(function(){
        centerBottom('#bottom_div');
    });

	</script>

	</body>
</html>

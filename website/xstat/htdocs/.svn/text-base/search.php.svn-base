<?php
require('common.inc.php');
page_start();
if (is_post()) {
	$_SESSION['search'] = $_POST['search'];
	redirect($_PAGE['.']);
}
$sth = db_query('SELECT oem_name,category,`key`,MIN(id) AS id FROM raw_data WHERE `key` like ? GROUP BY oem_name,category,`key`', array('%'.$_SESSION['search'].'%'));
?>
<html>
<head>
<style type="text/css">
	.main_table { border-collapse: collapse; }
</style>
</head>
<body>
<table border="1" class="main_table">
<thead><tr>
<td>OEM Name</td>
<td>Category</td>
<td>Key</td>
<td></td>
</tr></thead>
<tbody>
<?while ($row = db_fetch_assoc($sth)) {?>
<tr>
<td><?=$row['oem_name']?></td>
<td><?=$row['category']?></td>
<td><?=$row['key']?></td>
<td><a href="view2.php?id=<?=$row['id']?>">View</a></td>
</tr>
<?}?>
</tbody>
</table>
</body>
</html>
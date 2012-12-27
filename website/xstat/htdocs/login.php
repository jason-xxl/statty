<?php
require('common.inc.php');
page_start();
if (is_post()) {
	unset($GLOBALS['_PAGE']['.']);
	$id = NULL;
	//$host = '{auth.i.mozat.com:143}INBOX';
	$host = '{auth.i.mozat.com:143/novalidate-cert}';
	if (ends_with($_POST['login'], '@mozat.com')) {
		//if ($mail=@imap_open($host,$_POST['login'],$_POST['passwd'])) {
		if ($mail=@imap_open($host,substring_before($_POST['login'], '@'),$_POST['passwd'], OP_HALFOPEN, 1)){
			imap_close($mail);
			if (!($id=db_query_for_one('SELECT id FROM user WHERE name=?', array($_POST['login'])))) {
				db_query('INSERT INTO user (name,email) VALUES (:a,:a)', array('a'=>$_POST['login']));
				$id = db_last_insert_id();
			}
		}
	} else {
		$id = db_query_for_one('SELECT id FROM user WHERE name=? AND pw=MD5(?)', array($_POST['login'],$_POST['passwd']));
	}
	if ($id) {
		sess_set_uid(to_int($id), time());
		$_SESSION['U']['GROUPS'] = db_query_for_col("SELECT group_id FROM user_to_group WHERE user_id=?", array(sess_get_uid()));
		if($_POST['redirect']){
			redirect($_POST['redirect']);
		}
		if(ends_with($_POST['login'], '@mozat.com')){
			redirect(ors($_SESSION['_REDIRECT_FROM_'], 'home.php'));
		}else{
			redirect(ors($_SESSION['_REDIRECT_FROM_'], 'user_view.php'));
		}
	} else {
		$error = 'Login failed.';
	}
}
?>
<html>
<head>
<title>Gumi.SG Statistics</title>
</head>
<body>
<h3>Statistics Portal</h3>
<form action="login.php" method="post">
<table>
<tr><td>User: </td><td><input type="text" name="login"/></td></tr>
<tr><td>Password: </td><td><input type="password" name="passwd"/></td></tr>
<tr><td align="center"><font color="red"><?=$error?></font></td></tr>
<tr><td colspan="2"><input type="submit" value="Login"/><input type="hidden" name="redirect" value="<?=$_GET['redirect']?>"/></td></tr>
</table>
</form>
</body>
</html>

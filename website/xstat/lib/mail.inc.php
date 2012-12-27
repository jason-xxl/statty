<?
function mail_init() {
	global $_CONFIG,$_PAGE;
	include_once('Mail.php');
	if ($_CONFIG['mail']['host']) {
		$_PAGE['SMTP'] =& Mail::factory("smtp", $_CONFIG['mail']);
	} else {
		$_PAGE['SMTP'] =& Mail::factory("mail");
	}
}
function mail_footer() {
	global $_CONFIG;
	return is_array($_CONFIG['mail']['footer']) ? join("\n", $_CONFIG['mail']['footer']) : 
		$_CONFIG['mail']['footer'];
}
function mail_header() {
	global $_CONFIG;
	return array(
		"Sender" => $_CONFIG['mail']['sender']);
}
function get_rfc822_address($address, $personal = NULL) {
	if (is_array($address))
		return get_rfc822_address($address['address'],$address['personal']);
	return trim(nstr('"%s" ',$personal).nstr('<%s>',$address));
}
function sendmail($from, $to, $subject, $mail_body, $cc = array(), $bcc = array(), $images = array()) {
	global $_PAGE;
	$mail_body .= "\n\n".mail_footer();
	$headers = mail_header();
	$headers["From"] = get_rfc822_address($from);
	$headers["To"] = join(',',array_map('get_rfc822_address',$to));
	$headers["Reply-To"] = get_rfc822_address($from);;
	$headers["Subject"] = $subject;
	if ($cc)
		$headers["Cc"] = join(',',array_map('get_rfc822_address',$cc));
	if ($images) {
		include_once('Mail/mime.php');
		$mime = new Mail_mime();
		foreach ($images as $i => $image)
			$mime->addHTMLImage($image, 'image/png', preg_replace('/(\w+)$/e','hash("crc32","\1").".png"',$image));
		$mime->setHTMLBody($mail_body);
		$mime->setTxtBody("Your email client does not support HTML messages");
		$mail_body = $mime->get(array('text_charset'=>'UTF-8','html_charset'=>'UTF-8','head_charset'=>'UTF-8'));
		$headers = $mime->headers($headers);
	}
	$recipients = array_unique(array_map('get_rfc822_address',array_merge($to, $cc, $bcc)));
	$result = $_PAGE['SMTP']->send($recipients, $headers, $mail_body);
	if (PEAR::isError($result)) { 
		error_log($result->getMessage());
		if ($result->getCode() == 10005)
			return 422;
		elseif ($result->getCode() == 10001)
			return 500;
		elseif ($result->getCode() == 10002)
			return 425;
		else
			return 521;
	}
	return 0;
}
mail_init();
?>
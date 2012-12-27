$.defaultLoader = {css: {backgroundColor: '#333'}, message: '<table align="center"><tr><td width="150" height="40" align="center">Please wait... <img src="images/ajax-loader2.gif" align="absmiddle"/></td></tr></table>'};
function init_form() {
	var t = $('input[name=page]',this).val();
	if ($.validator)
		$(this).validate();
	$(this).ajaxForm({
		target: t ? t.replace(/^(?!#)/, "#subpage_") : 'body',
		beforeSubmit: on_submit,
		success: on_response
	});
	$(':submit',this).click(function() {
		$(this.form).data('srcElement',this);
		return true;
	});
}
function on_submit(formData, $form, options) {
	var srcElement = $form.data('srcElement') || this;
	var opt = $(srcElement).metadata() || {};
	$.extend(true, opt, $form.data('options') || {});
	$form.removeData('srcElement').removeData('options');
	if (opt.confirm && !window.confirm(opt.confirm))
		return false;
	for (var k in opt.beforeSubmit || []) {
		if (!window[k].apply(srcElement, opt.beforeSubmit[k]))
			return false;
	}
	for (var k in opt.post || [])
		formData.push({name: k, value: opt.post[k]});
	if (opt.target)
		options.target = opt.target;
	$('.page_msg').hide();
	if ($.blockUI && !opt.noblock)
		$form.block($.defaultLoader);
	return true;
}
function on_response(responseText, statusText, xhr, $form) {
	if (window.on_load && typeof window.on_load == 'function')
		on_load.apply(this);
	if ($.fn.ajaxSubmit)
		$('form:not(.noinit)',this).each(init_form);
}
function close_dialog(btn) {
	$(btn || this).closest('.ui-dialog-content').dialog('close');
}
function load() {
	if ($.validator) {
		$.validator.setDefaults({
			ignore: ':button',
			errorPlacement:  function(error, element) {
				if ($('span[for='+element.attr('name').replace(/([\[\]:])/g, '\\$1')+']').append(error).size() == 0)
					element.after(error);
			} 
		});
	}
	if ($.blockUI) {
		$.blockUI.defaults['css'] = {width:'30%', top: '40%', left: '35%', textAlign: 'center'};
		$.blockUI.defaults['message'] = null;
	}
}
load();
$(document).ready(function() {
	on_response.apply(document);
});

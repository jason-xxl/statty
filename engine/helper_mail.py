import _mysql
import helper_regex
import config
import sys

import smtplib
from email.mime.text import MIMEText

from qlx_src.helper import email_

def createhtmlmail (html, text, subject, mail_from=''):
    """Create a mime-message that will render HTML in popular
       MUAs, text in better ones"""
    import MimeWriter
    import mimetools
    import cStringIO
    
    out = cStringIO.StringIO() # output buffer for our message 
    htmlin = cStringIO.StringIO(html)
    txtin = cStringIO.StringIO(text)
    
    writer = MimeWriter.MimeWriter(out)
    #
    # set up some basic headers... we put subject here
    # because smtplib.sendmail expects it to be in the
    # message body
    #
    if mail_from:
        writer.addheader("From", mail_from)
    writer.addheader("Subject", subject)
    writer.addheader("MIME-Version", "1.0")
    #
    # start the multipart section of the message
    # multipart/alternative seems to work better
    # on some MUAs than multipart/mixed
    #
    writer.startmultipartbody("alternative")
    writer.flushheaders()
    #
    # the plain text section
    #
    subpart = writer.nextpart()
    subpart.addheader("Content-Transfer-Encoding", "quoted-printable")
    pout = subpart.startbody("text/plain", [("charset", 'us-ascii')])
    mimetools.encode(txtin, pout, 'quoted-printable')
    txtin.close()
    #
    # start the html subpart of the message
    #
    subpart = writer.nextpart()
    subpart.addheader("Content-Transfer-Encoding", "quoted-printable")
    #
    # returns us a file-ish object we can write to
    #
    pout = subpart.startbody("text/html", [("charset", 'us-ascii')])
    mimetools.encode(htmlin, pout, 'quoted-printable')
    htmlin.close()
    #
    # Now that we're done, close our writer and
    # return the message body
    #
    writer.lastpart()
    msg = out.getvalue()
    out.close()
    print msg
    return msg


def send_mail(title='',content_txt='',content_html='',target_mails=config.mail_targets,source_mail=config.mail_from):
    msg= createhtmlmail(content_html, content_txt, title, source_mail)
    s = smtplib.SMTP(config.smtp_server)
    s.sendmail(source_mail, target_mails, msg)
    s.quit()

def send_mail_new(title='',content_txt='',content_html='',target_mails=config.mail_targets,source_mail=config.mail_from):
    if content_txt:
        raise Exception('only html is supported.')
    return email_.send(from_addr=source_mail, to_addrs=target_mails, subject=title, \
                       content=content_html, server=config.smtp_server)
    
send_mail = send_mail_new # use new   
 
if __name__ =='__main__':
    send_mail(title="title",content_html="content <a href='http://163.com'>f</a>")
    


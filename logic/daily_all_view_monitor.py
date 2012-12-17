'''
Created on May 17, 2012

@author: Mozat
'''

import urllib, urllib2
import re
import helper_regex, helper_file
import collections
import time, datetime
import helper_mail
import socket

"""
Process Procedure:
1 Given a list of user accounts, websites and time durations.  
3 Identify zero or invalid values.  
3 Send Email

todo:
1 Monthly should be treated specially. the time is a range. Ensure last month is checked.  
2 Read Configure File: URL and some parameters, some  % &date1=2012-04-19&date2=2012-05-17
"""

# settings
date_period = [-3,-1]
month_period = [-1] # must have last month
timeout = 100*60 # 10minutes

# zoota 
zoota_web_login_url = 'http://xstat.admin.zoota.vn/xstat/login.php'
zoota_web_login_account = { 'login':'stat_monitor', 'passwd':'m0z@tst@t','page_lang_select':'en'}
zoota_web_view_base_url = 'http://xstat.admin.zoota.vn/xstat/view.php'
zoota_web_view_ids = [1183,1190,1194,1186,1189,1191,1187,1196,1199,1192,1197,1198,1195]
 

# stat portal
stat_web_login_url = 'http://statportal.morange.com/xstat/login.php'
stat_web_login_account = { 'login':'stat_monitor', 'passwd':'m0z@tst@t' }
stat_web_view_base_url = 'http://statportal.morange.com/xstat/view.php'
stat_web_shabik_view_ids = [903, 1080, 767, 899, 1182, 919, 1015, 1092, 1089, 1093, 917, 1011, 3, 905, 1018, 1114, 769, 1083, 906, 913]
stat_web_vodafone_view_ids = [1081, 1068, 1061, 705, 783, 1064, 1071, 699, 695, 1070, 857, 1119, 772, 706, 696] #697, 
stat_web_mozat_view_ids = [353, 742, 388, 1122, 692]
stat_web_globe_view_ids = [944, 1000, 940, 1010, 1003, 949, 1084, 954, 951]
stat_web_uMobile_view_ids = [1109, 1108, 1098, 1099, 1111, 1112, 1110]
stat_web_ais_view_ids = [1142, 1153, 1166, 1140, 1148, 1141, 1165, 1144, 1145,1143] 
stat_web_umniah_view_ids = [284, 291, 282, 457, 289]
stat_web_viva_view_ids = [27, 455, 110, 115]
stat_web_viva_bh_view_ids = [29, 456, 122, 127]
stat_web_telk_armor_view_ids = [324]
#stat_web_report_vodafone = [725,1062, 694, 704, 703, 797, 698, 693, 1063, 700, 701]
stat_web_report_vodafone = [725, 1105, 1107, 727, 728, 729, 730, 731, 763, 760, 1106, 786, 787]
#stat_web_mozat_view_ids = []
#stat_web_mozat_view_ids = [] 
#stat_web_mozat_view_ids = [] 
stat_web_view_ids = [stat_web_shabik_view_ids, stat_web_vodafone_view_ids, stat_web_mozat_view_ids,\
                     stat_web_globe_view_ids,stat_web_uMobile_view_ids,\
                     stat_web_ais_view_ids,stat_web_umniah_view_ids,stat_web_viva_view_ids,\
                     stat_web_viva_bh_view_ids,stat_web_telk_armor_view_ids,\
                     stat_web_report_vodafone]
#stat_web_view_ids  = [stat_web_report_vodafone]
stat_web_view_ids = [vid for vids in stat_web_view_ids for vid in vids]
print stat_web_view_ids
 
# error code
CODE_ERROR = -1 # page has error 
CODE_NO_CONTENT = -2 # no content in the given url
ID_TITLE_MAPPING = {} 
 
def get_http_response(url, get_params={}, post_params=None, headers={}, timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
    if get_params:
        url = url + "?%s" % urllib.urlencode(get_params)
    if post_params:
        post_params = urllib.urlencode(post_params)
    print url, get_params,post_params,headers, timeout
    return urllib2.urlopen(urllib2.Request(url,post_params,headers),None,timeout)

def get_http_content(url, get_params={}, post_params=None, headers={}):
    return get_http_response(url, get_params, post_params, headers).read()
 
def getSessionID(url, account_dict):
    """
    Need to connect twice to obtain the session ID
    1) connect to obtain the initial session ID by extracting it from the cookie
    2) register the session ID by sending the user name and password
    
    Return None if fail, and the session ID if passed all information is correct.  
    """
    ## step 1: get initial session ID from the cookie
    resp = get_http_response(url)
    cookie = resp.info()['Set-Cookie']
#    print resp.getcode()
#    print resp.info()
#    print resp.read()
    PHPSESSID = helper_regex.extract(cookie,r'PHPSESSID=(.+);').strip()
    
    # step 2: register the session ID
    resp = get_http_response(url,{},account_dict,{"Cookie": 'PHPSESSID=%s' % PHPSESSID})
    content = resp.read()
#    print resp.info()
    #print content
    if content.find("Login failed") > 0:
        return None
    else:
        return PHPSESSID
        
        
def valid_data_in_url(url, vid, date1, date2, PHPSESSID):
    """
    Step 1: valid_data_in_url has content or not
    Step 2: extract the fields
    Step 3: extract the data
    Step 4: detect invalid data 
    Step 5: 
    """
    if date1 and date2:
        d = get_http_content(url, {'id': vid}, {"date1":date1, "date2":date2}, {"Cookie": 'PHPSESSID=%s' % PHPSESSID})
    else:
        d = get_http_content(url, {'id': vid}, None, {"Cookie": 'PHPSESSID=%s' % PHPSESSID})

    # get title
    title = helper_regex.extract(d,r'<title>(.+)</title>')
#    print vid ,'-> ' ,title
    ID_TITLE_MAPPING[vid] = title
    
    # Step 1: valid_data_in_url empty content
    if d.find('Stack trace') >0:
        return CODE_ERROR
    elif d.find("</td>") < 0:
        return CODE_NO_CONTENT
    
    # Step 2: extract fields 
    # <th>Time</th>
    fields = []
    num_fields = -1
    lines = re.findall("<th>.*", d)
    for line in lines:
        value = helper_regex.extract(line,r'<th>(.+)</th>')
        fields.append(value)
#        print line.rstrip(),'->',value
    num_fields = len(fields) 
#    print lines
    print len(fields), fields
    
    # Step 3: extract data 
    # data
    # <td><span class="hidden"></span>3</td>
    # <td><span class="hidden"></span><a href="./downloadcol.py?collection_id=6725142">90116</a></td>
    # <td><span class="hidden"></span><b>0</b></td>
    # <td><span class="hidden"></span><span>0.000</span></td>
    matrix = []
    num_data = -1
    lines = re.findall(".*</td>", d) # // 
    #print lines
    #print len(lines)
    for line in lines:
        value = helper_regex.extract(line,r'</span>(.+)</td>').strip()
#        print value
        if value.find('href') > 0: # 
            value = helper_regex.extract(value,r'">(.+)</a>').strip()
        elif value.find("</b>") > 0:
            value = helper_regex.extract(value,r'<b>(.+)</b>').strip()
        elif value.find("</span>") > 0:
            value = helper_regex.extract(value,r'<span>(.+)</span>').strip()
        matrix.append(value)
#        print line.rstrip(),'->',value
    num_data = len(matrix)
    
    # valid_data_in_url
    if 0 != num_data % num_fields:
        print 'num_fields: ', num_fields, 'number data: ', num_data
        raise ValueError("data format error") # send email
    num_rows = num_data / num_fields
#    print num_rows
    
    first_column = matrix[0:num_data:num_fields] # the first column, can be timr or date
#    print first_column
    
    # Step 4: detect invalid data (zero data) 
#    report = []
#    report = collections.defaultdict(dict)
#    report = collections.OrderedDict()
#    str = ''
    cnt = 0
    for i in range(0,num_rows):
#        row_report = []
#        row_str = ''
        for j in range(0,num_fields):
            #### skip
            if 0 == j: # skip the first column when it is time
                if fields[0] == 'Time': 
                    continue
                elif fields[0] == 'Date': 
                    continue  
                else:
                    raise ValueError('the first column is not Time or Date')
            elif fields[j] == 'User Group' and (vid == 1080 or vid == 1081):  # id 1080,
                continue
            elif (fields[j] == 'Online-Time Avg Daily' or fields[j]=='Online-Time Avg Per Login-Session'):# and (vid == 913 or vid==954 or vid==1112 or vid==1145):
                continue
            elif fields[j] == 'Country': # for vid 742
                continue
            elif fields[j] == 'Download' and (vid ==725 or vid==727):
                continue
            elif fields[j] == 'Price Type' and vid ==1107:
                continue
                
            value = matrix[i*num_fields+j]
            
            if '-' == value:
                cnt += 1
                continue

#            print vid, i,',', j, '', fields[j], '->', value 
            value = value.replace(",", '').replace('%','') # remove ,
            value = float(value)

            if value <= 1e-10:
                cnt += 1
#                row_str += fields[j]
#                    report[i][j]
#                report.append((i,j))
#        report.append()
#        print matrix[i*num_fields:(i+1)*num_fields] # print row
#        xx = ''
#        for j in range(0,num_fields):
#            xx += ' '+matrix[i*num_fields+j]
#        print xx,'\n'
    
    # mail
    msg = ''
    if cnt>0:
        msg = 'Invalid Data Detected!' 
    else:
        msg = 'OK.'
    
    print '------',url+'?id=%s' % vid, msg, cnt
            
    return cnt


def check_sercice(service):
    if service == 'zoota':
        login_url = zoota_web_login_url
        login_account = zoota_web_login_account
        url = zoota_web_view_base_url
        ids = zoota_web_view_ids
    
    elif service == 'stat':
        login_url = stat_web_login_url
        login_account = stat_web_login_account
        url = stat_web_view_base_url
        ids = stat_web_view_ids
    else:
        raise ValueError('Unsupported string %s' % str)
    
    # get session ID
    PHPSESSID = getSessionID(login_url, login_account)
    print 'Session ID: ',PHPSESSID 
    if not PHPSESSID:
        msg = '<font color=red>Cannot obtain the session ID: username or password is incorrect!</font>'
        print msg
        helper_mail.send_mail(title=helper_file.get_current_script_file_name(),content_html=msg) 
        exit()
        
    current_date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
    date1 = helper_regex.date_add(current_date, date_period[0])
    date2 = helper_regex.date_add(current_date, date_period[1])

    def geturl(url,vid):
        newurl = url + "?%s" % urllib.urlencode({'id':vid})
        print ID_TITLE_MAPPING, vid
        return "<a href='%s'>%s</a>" % (newurl, ID_TITLE_MAPPING.get(vid) if ID_TITLE_MAPPING.has_key(vid) else vid)
#        return "<a href='%s'>%s</a>" % (newurl, vid)
        
    bad_urls = [] 
    no_content_urls = []
    bad_cnts = []
    error_urls = []
    for vid in ids:
#        if 1198 == vid: continue ### test it later monthly
    
        # no content
#        if 1182 == vid: continue # Shabik 360 - Chatroom New Daily  
#        elif 919 == vid: continue # has exception  
#        elif 1114 == vid: continue # Shabik 360 - New User Homepage Init Daily
#        elif 783 == vid: continue # Vodafone - Download, not formated
#        elif 1064 == vid: continue # Vodafone - Download Daily, nothing

        cnt = int(valid_data_in_url(url,vid,date1,date2, PHPSESSID))
#        newurl = url + "?%s" % urllib.urlencode({'id':vid})
        newurl = geturl(url,vid)
        if CODE_NO_CONTENT == cnt: # no data
            no_content_urls.append(newurl)
        elif CODE_ERROR == cnt:
            error_urls.append(newurl)
        elif cnt > 0: # invalid data
            bad_urls.append(newurl)
            bad_cnts.append(cnt)
        elif cnt == 0: # ok
            pass
    
        
    sep = '<br/>\n'
#    sep = '\n'
    msg = sep+sep+'<font color=red>'+service+'</font>'
    
    msg += sep+'<b>Error URLs</b>: '+sep+ sep.join(error_urls)
    msg += sep+'<b>Empty URLs</b>: '+sep+ sep.join(no_content_urls)
    msg += sep+'\n <b>Bad URLs and Counts</b>: '+sep+ sep.join([url+'\t'+str(cnt) for url,cnt in zip(bad_urls,bad_cnts)])
#    print msg
    return msg

if __name__ == '__main__':
    # test
    #import config
    #config.smtp_server='ismtp.mozat.com'
    #config.smtp_server='smtp.mozat.com'
    #config.mail_targets = ['xiangqiaoliang@mozat.com']
    
    msg = ''
    msg += check_sercice('zoota')
    msg += check_sercice('stat')
    print msg
    
    #Send Email
    helper_mail.send_mail(title=helper_file.get_current_script_file_name(),content_html=msg)    
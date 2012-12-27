import pygeoip
import config
import socket

gi = pygeoip.GeoIP(config.get_file_path_with_correct_slash(config.execute_dir+r'/GeoIP.dat'))

def get_country_name_from_ip(ip_str):
    try:
        country=gi.country_name_by_addr(ip_str)
    except:
        print 'Wrong IP: ',ip_str
        return 'Wrong IP'
    return country

def get_country_code_from_ip(ip_str):
    try:
        country=gi.country_code_by_addr(ip_str)
    except:
        print 'Wrong IP: ',ip_str
        return 'Wrong IP'
    return country

def get_country_name_from_host_name(host_name):
    try:
        country=gi.country_name_by_name(host_name)
    except:
        print 'Wrong Host Name: ',host_name
        return 'Wrong Host Name'
    return country

def get_country_code_from_host_name(host_name):
    try:
        country=gi.country_code_by_name(host_name)
    except:
        print 'Wrong Host Name: ',host_name
        return 'Wrong Host Name'
    return country

def get_current_server_ip():
    #ip_win=socket.gethostbyname(socket.gethostname())
    #if ip_win:
     #   return ip_win

    ip_linux=''
    tmp_socket= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tmp_socket.connect(("google.com",80))
    ip_linux=tmp_socket.getsockname()[0]
    tmp_socket.close()

    return ip_linux
    

if __name__=='__main__':
    ip='188.229.221.223'
    print get_country_name_from_ip(ip)
    print get_country_code_from_ip(ip)
    print get_country_name_from_host_name('google.com')
    print get_country_code_from_host_name('google.com')
    print get_current_server_ip()

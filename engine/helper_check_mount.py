import os, os.path
import socket
import helper_ip

def check_mount():
	server_ip = helper_ip.get_current_server_ip()
	directory = '/mnt/%s/'%[server_ip]
	if os.path.exists(directory):
		if locate('*.log', directory):
			return True
		else:
			mount(server_ip, False)
	else:
		mount(server_ip, True)
	return "a"

def locate(pattern, dir):
	text_files = [f for f in os.listdir(dir) if f.endswith(pattern)]
	if text_files:
		return True
	return False

def mount(server_ip, mkdir):
	try:
		if(mkdir):
			os.cmd('mkdir ~/mnt/%s sudo mount -t cifs //%s/MyShare /mnt/%s/ -o username=smbuser,passwd=smbpassword'%[server_ip, server_ip, server_ip])
		else:
			os.cmd('sudo mount -t cifs //%s/MyShare /mnt/%s/ -o username=smbuser,passwd=smbpassword'%[server_ip, server_ip, server_ip])
		return True
	except:
		return False

if __name__=='__main__':
	print 'MOUNT:'
	print check_mount()
    #print check_mount()

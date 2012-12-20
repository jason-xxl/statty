import os, os.path
import socket
import helper_ip

def check_mount():
	server_ip = helper_ip.get_current_server_ip()
	directory = '/mnt/%s'%server_ip
	if os.path.exists(directory):
		if locate('.log', directory):
			return True
		else:
			return mount(server_ip, False)
	else:
		return mount(server_ip, True)

def locate(pattern, dir):
	text_files = [f for f in os.listdir(dir) if f.endswith(pattern)]
	if text_files:
		return True
	return False

def mount(server_ip, mkdir):
	directory = '/mnt/%s'%server_ip
	try:
		if(mkdir):
			if not os.path.exists('/mnt'): 
				os.system('sudo mkdir /mnt/')
			os.system('sudo mkdir /mnt/%s'%(server_ip))
			os.system('sudo mount -t cifs //%s/MyShare /mnt/%s/ -o username=smbuser,passwd=smbpassword'%(server_ip, server_ip))
		else:
			os.system('sudo mount -t cifs //%s/MyShare /mnt/%s/ -o username=smbuser,passwd=smbpassword'%(server_ip, server_ip))
		if not locate('.log', directory):
			return False
		return True
	except Exception as exception:
		return False

if __name__=='__main__':
	print 'MOUNT:'
	print check_mount()
    #print check_mount()

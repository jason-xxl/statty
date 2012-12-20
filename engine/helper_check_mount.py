import os, os.path
import socket
import helper_ip

def check_mount():
	server_ip = helper_ip.get_current_server_ip()
	directory = '/mnt/%s'%server_ip
	if os.path.exists(directory):
		print 'file exist \n'
		if locate('.log', directory):
			return True
		else:
			return mount(server_ip, False)
	else:
		return mount(server_ip, True)

def locate(pattern, dir):
	print 'locate file \n'
	text_files = [f for f in os.listdir(dir) if f.endswith(pattern)]
	print text_files
	if text_files:
		print 'True\n'
		return True
	print 'False\n'
	return False

def mount(server_ip, mkdir):
	print 'mount\n'
	try:
		if(mkdir):
			if not os.path.exists('/mnt'): 
				os.system('sudo mkdir /mnt/')
			os.system('sudo mkdir /mnt/%s sudo mount -t cifs //%s/MyShare /mnt/%s/ -o username=smbuser,passwd=smbpassword'%(server_ip, server_ip, server_ip))
		else:
			os.system('sudo mount -t cifs //%s/MyShare /mnt/%s/ -o username=smbuser,passwd=smbpassword'%(server_ip, server_ip))
		return True
	except Exception as exception:
		print exception
		return False

if __name__=='__main__':
	print 'MOUNT:'
	print check_mount()
    #print check_mount()

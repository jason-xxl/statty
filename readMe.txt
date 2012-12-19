1. git clone "https://github.com/jason-xxl/statty.git"
2. mkvirtualenv "statty"
3. pip install -r requirement.txt
4. create databases from mysql workbench or command line
	CREATE DATABASE statty;
5. python manage.py syncdb
6. python manage.py runserver 8001

For Addtional 
- CREATE DATABASE statty_data;

Create one folder called data which containe two subfolder collection and named_collection.

Celery Usage
============
Install django-celery
git clone git://github.com/celery/django-celery.git

Changes to setting.pys
import djcelery

djcelery.setup_loader()

BROKER_HOST = "localhost"
BROKER_PORT = 5672
BROKER_USER = "root"
BROKER_PASSWORD = "gumi.asia123"
BROKER_VHOST = "8001"

cd statty
python manage.py celeryd_detach //running at background
___________________________________________________________________

Install rabbitmq
http://www.rabbitmq.com/install-mac.html
- Download rabbitmq-server-generic-unix-3.0.1.tar.gz
- tar -xzvf rabbitmq-server-generic-unix-3.0.1.tar.gz
- cd rabbitmq_server-3.0.1
- sudo rabbitmq-server //running at outside
		or
- sbin/rabbitmq-server -detached //running at background
- sbin/rabbitmqctl stop

Setting up RabbitMQ
http://ask.github.com/celery/getting-started/broker-installation.html
- cd rabbitmq_server-3.0.1/sbin/
- abbitmqctl add_user root gumi.asia123
- rabbitmqctl add_vhost 8001
- rabbitmqctl set_permissions -p 8001 root ".*" ".*" ".*"
- sudo scutil --set HostName winhnin.local

Configuring django porject to use celery



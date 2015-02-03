# App Fingerprint
Identify apps from their traffic flows

## Dependencies

### pyshark
Before installing pyshark, you need to install a bunch of libraries needed by pyshark. If you are using ubuntu, the following command may be helpful.

	sudo apt-get install libxml2-dev libxslt1-dev python-dev
	
To install pyshark

	wget https://github.com/KimiNewt/pyshark/zipball/master -O pyshark.zip
	unzip pyshark.zip
	cd KimiNewt-pyshark-26f21f5/src
	python setup.py install
	
	
	
### Mysql
To install mysql connector

Please Visit http://dev.mysql.com/downloads/connector/python/ and download the package for your platform

def x_requets_classifer(package):
	if 'X-Requested-With' in package.add_header:		
		return package.add_header.replace('X-Requested-With','')
	return None

def bundle_id(package):
	if u'bundle_id' in package.querys:		
		return package.querys['bundle_id'][0]
	return None

def app_id(package):
	if u'app_id' in package.querys:		
		return package.querys['app_id'][0]
	return None

def app_name(package):
	if u'app_name' in package.querys:		
		return package.querys['app_name'][0]
	return None

def appid(package):
	if u'appid' in package.querys:		
		return package.querys['appid'][0]
	return None

def source_appstore_id(package):
	if u'source_app_store_id' in package.querys:
		return package.querys['appid'][0]
	return None

def classify():
	import mysql.connector
	from package import Package
	import urllib
	cnx=mysql.connector.connect(user='root',password='123',host='127.0.0.1',database='fortinet')
	cursor = cnx.cursor()
	query = "select app, add_header,path from packages_bak"
	cursor.execute(query)
	classifiers = [x_requets_classifer,bundle_id, app_id, app_name, appid]
	regapps = set()
	for app, add_header, path in cursor:
		package = Package()
		package.set_path(urllib.unquote(path))
		package.set_add_header(add_header)
		# print package.querys
		for classifier in classifiers:
			identifier = classifier(package)
			if identifier != None and identifier != '':
				regapps.add(app)
	for app in regapps:
		print app
classify()

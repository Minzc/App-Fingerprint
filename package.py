class Package:
	def set_path(self, path):
		import urlparse
		self.querys = urlparse.parse_qs(urlparse.urlparse(path).query, True)
		self.path = path

	def set_add_header(self, add_header):
		self.add_header = add_header
class Package:
    def set_id(self, id):
        self.id = id

    def set_dst(self, dst):
        self.dst = dst

    def set_name(self, app_name):
        if app_name:
            packapp_name = app_name.lower().replace('-', ' ').replace('_', ' ').replace(',', ' ')
        self.name = app_name

    def set_company(self, company):
        if company: company = company.lower()
        self.company = company

    def get_name(self):
        return self.name

    def set_app(self, app):
        self.app = app.lower()

    def set_refer(self, refer):
        import urlparse
        import urllib

        path = urllib.unquote(path).lower().replace(';', '?', 1).replace(';', '&')
        self.origPath = path
        self.querys = urlparse.parse_qs(urlparse.urlparse(path).query, True)
        self.path = urlparse.urlparse(path).path

    def set_path(self, path):
        import urlparse
        import urllib

        path = urllib.unquote(path).lower().replace(';', '?', 1).replace(';', '&')
        self.origPath = path
        self.querys = urlparse.parse_qs(urlparse.urlparse(path).query, True)
        self.path = urlparse.urlparse(path).path

    def set_add_header(self, add_header):
        self.add_header = add_header.lower()

    def set_refer(self, refer):
        self.refer = refer

    def set_host(self, host):
        import tldextract

        host = host.lower()
        self.host = host.split(':')[0].replace('www.', '')
        extracted = tldextract.extract(host)
        self.secdomain = None

        if len(extracted.domain) > 0:
            self.secdomain = "{}.{}".format(extracted.domain, extracted.suffix)

    def set_agent(self, agent):
        self.agent = agent.lower()
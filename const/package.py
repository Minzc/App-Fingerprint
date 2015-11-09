import urlparse
import urllib
class Package:
  def __init__(self):
    self.json = None
    self.form = None
  
  def set_tbl(self, tbl):
    self.tbl = tbl

  def set_label(self, label):
    self.label = label

  def set_id(self, id):
    self.id = id

  def set_dst(self, dst):
    self.dst = dst

  def set_appinfo(self, appInfo):
    self.appInfo = appInfo

  @property
  def name(self):
    return self.appInfo.name

  @property
  def app(self):
    return self.app
  
  @property
  def category(self):
    return self.appInfo.category

  @property
  def company(self):
    return self.appInfo.company

  @property
  def website(self):
    return self.appInfo.website

  def set_app(self, app):
    self.app = app.lower()

  def set_refer(self, refer):
    url = urllib.unquote(refer).lower().replace(';', '?', 1).replace(';', '&')
    parsed_url = urlparse.urlparse(url)
    query = urlparse.parse_qs(urlparse.urlparse(url).query, True)
    host = parsed_url.netloc
    path = parsed_url.path
    self.refer_host = host.split(':')[0].replace('www.', '').replace('http://','')
    self.refer_queries = query

  def set_path(self, path):

      path = urllib.unquote(path).lower().replace(';', '?', 1).replace(';', '&')
      self.origPath = path
      self.queries = urlparse.parse_qs(urlparse.urlparse(path).query, True)
      self.path = urlparse.urlparse(path).path

  def set_add_header(self, add_header):
      self.add_header = add_header.lower()


  def set_host(self, host):
      import tldextract

      host = host.lower()

      self.host = host.split(':')[0].replace('www.', '').replace('http://','')
      extracted = tldextract.extract(host)
      self.secdomain = None

      if len(extracted.domain) > 0:
          self.secdomain = "{}.{}".format(extracted.domain, extracted.suffix)

  def set_agent(self, agent):
      self.agent = agent.lower()

  def set_content(self, content):
    content = content.lower()
    if 'layer json' in content:
      self.json = self._process_json(content)
    if 'layer urlencoded-form' in content:
      self.form = self._process_form(content)
    self.content = content

  def _process_form(self, content):
    """change urlencoded forms to maps"""
    key_values = {}
    for line in filter(None, content.strip().split('\n')):
      if 'form item' in line:
        line = line.replace("\"", '').replace('form item:', '').replace(' ', '')
        if '=' in line: 
          key, value = line.split('=')[:2]
          key_values[key.strip()] = value.strip()
    return key_values

  def _process_json(self, content):
    """change json content to string items"""
    items = []
    for line in filter(None, content.split('\n')):
      if 'value' in line and ':' in line:
        items.append(':'.join(map(lambda seg: seg.strip(), line.split(':')[1:])))
    return items

  @app.setter
  def app(self, value):
    self._app = value

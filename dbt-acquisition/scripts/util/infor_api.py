import json
import os
from dataclasses import dataclass
from re import Pattern

import requests

from scripts.util.logger import log

class InforAPIClient:
  def __init__(
    self,
    environment: str,
    auth_url: str,
    data_url: str,
    client_id: str,
    client_secret: str,
    username: str,
    password: str
  ):
    self.environment = environment
    self.auth_url = auth_url
    self.data_url = f'{data_url}/{environment}'
    self.client_id = client_id
    self.client_secret = client_secret
    self.username = username
    self.password = password
    
    self.route_object = f'{self.data_url}/DATAFABRIC/datacatalog/v1/object'

    self.session = requests.Session()
    self.token = None

    self.authenticate()

  def authenticate(self):
    try:
      log.info(f'Authenticating against {self.auth_url}')
      response = requests.post(self.auth_url, data={
        'grant_type': 'password',
        'client_id': self.client_id,
        'client_secret': self.client_secret,
        'username': self.username,
        'password': self.password
      })
      response.raise_for_status()
        
      data = response.json()
      self.token = data.get('access_token')
        
      self.session.headers.update({
        'Authorization': f'Bearer {self.token}',
        'Content-Type': 'application/json'
      })
      log.info('InforAPI Authentication successful')
        
    except requests.exceptions.RequestException as e:
      log.error(f'InforAPI Authentication failed: {e}')

  def get_object_list(self, query: Pattern = None) -> list[str]:
    log.info(f'Querying {self.route_object}/list')
    response = self.session.get(f'{self.route_object}/list').json()
    if 'error' in response:
      log.error(f'Infor API Error: {response['error']}')
      return
    if query:
      return [o['name'] for o in response['objects'] if query.match(o['name'])]
    return [o['name'] for o in response['objects']]

  def get_object_definition(self, name: str) -> dict:
    log.info(f'Querying {self.route_object}/{name}')
    response = self.session.get(f'{self.route_object}/{name}').json()
    try:
      if response['type'] != 'JSON':
        raise Exception(f'Object {name} is not JSON Schema')
    except Exception as e:
      log.error(f'Error parsing {name}: {e}')
      return
    return response


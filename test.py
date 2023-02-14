import requests

response = requests.get('https://api.github.com/search/users?q=arthurcol')

print(response.status_code)
print(response.json()['items'])

import json
import urllib.request

url = 'http://localhost:8080/incentive'
data = json.dumps({'senderId': 1, 'recipientId': 2, 'amount': 10.0}).encode('utf-8')
req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
with urllib.request.urlopen(req) as resp:
    print(resp.status)
    print(resp.read().decode('utf-8'))

import requests

params = {"msg": "we're using python from bazel!"}
# httpbin retuns back whatever you give it
response = requests.get("https://httpbin.org/get", params=params)
print(response.json()["args"]["msg"])

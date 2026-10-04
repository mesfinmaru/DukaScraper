import httpx, json, time
B="http://localhost:8000/api/v1"
t=httpx.post(f"{B}/auth/login", json={"email":"dukascraper@gmail.com","password":"Duka@12345"}).json()["access_token"]
h={"Authorization":f"Bearer {t}"}
for url in ["https://www.bbc.com/amharic","https://www.ebc.et/english","https://www.voanews.com/"]:
    r=httpx.post(f"{B}/jobs/trigger", headers=h, json={"url":url,"language":"am","max_depth":0})
    print(url, r.status_code, r.text[:200])

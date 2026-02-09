import os
import time
import requests
import feedparser
from dateutil import parser
from bs4 import BeautifulSoup
import json

# --- Configuration ---
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
FEISHU_BASE_TOKEN = os.environ.get("FEISHU_BASE_TOKEN")
FEISHU_TABLE_ID = os.environ.get("FEISHU_TABLE_ID")
RSS_FEEDS_ENV = os.environ.get("RSS_FEEDS", "")

feishu_api_base = "https://open.feishu.cn/open-apis"

def get_tenant_access_token():
    url = f"{feishu_api_base}/auth/v3/tenant_access_token/internal"
    payload = {"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()["tenant_access_token"]

def fetch_existing_records(token):
    url = f"{feishu_api_base}/bitable/v1/apps/{FEISHU_BASE_TOKEN}/tables/{FEISHU_TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    existing_links = set()
    page_token = None
    
    while True:
        params = {"page_size": 100}
        if page_token: params["page_token"] = page_token
        try:
            resp = requests.get(url, headers=headers, params=params).json()
            if resp.get("code") != 0:
                print(f"Error fetching records: {resp}")
                break
        except Exception as e:
            print(f"Fetch request failed: {e}")
            break
        
        if "data" in resp and "items" in resp["data"]:
            for item in resp["data"]["items"]:
                link_field = item["fields"].get("Link", "")
                if isinstance(link_field, dict):
                    existing_links.add(link_field.get("link", ""))
                else:
                    existing_links.add(link_field)
        
        if resp["data"].get("has_more"):
            page_token = resp["data"]["page_token"]
        else:
            break
    return existing_links

def parse_feeds(feed_urls, existing_links):
    new_records = []
    for url in feed_urls:
        if not url.strip(): continue
        try:
            print(f"Checking feed: {url}")
            feed = feedparser.parse(url)
            feed_title = feed.feed.get("title", "Unknown Source")
            
            for entry in feed.entries:
                link = entry.get("link", "")
                if link in existing_links: continue 
                
                existing_links.add(link)
                
                summary_raw = entry.get("summary", "") or entry.get("description", "")
                summary_text = BeautifulSoup(summary_raw, "html.parser").get_text()[:1000]
                
                pub_parsed = entry.get("published_parsed") or entry.get("updated_parsed")
                pub_ts = int(time.mktime(pub_parsed) * 1000) if pub_parsed else int(time.time() * 1000)

                record = {
                    "fields": {
                        "Title": entry.get("title", "No Title"),
                        "Link": {"text": "点击阅读", "link": link},
                        "Source": feed_title,
                        "Author": entry.get("author", feed_title),
                        "Summary": summary_text,
                        "Date": pub_ts
                    }
                }
                new_records.append(record)
        except Exception as e:
            print(f"Error parsing {url}: {e}")
    return new_records

def batch_create_records(token, records):
    if not records: return
    # 注意：这里的 batch_create 后面没有 records/batch_create，标准 API 路径是这个
    # 之前可能用的是旧文档路径，但大多数情况是通用的。这里我们用标准 v1 接口
    url = f"{feishu_api_base}/bitable/v1/apps/{FEISHU_BASE_TOKEN}/tables/{FEISHU_TABLE_ID}/records/batch_create"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    batch_size = 100
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        payload = {"records": batch}
        
        try:
            print(f"Sending batch of {len(batch)} records...")
            response = requests.post(url, headers=headers, json=payload)
            data = response.json()
            
            # --- 强制打印飞书返回的任何信息 ---
            print(f"DEBUG: Status Code: {response.status_code}")
            print(f"DEBUG: Feishu Response: {json.dumps(data, ensure_ascii=False)}")
            # -------------------------------

            if data.get("code") == 0:
                print(f"Successfully added {len(batch)} records.")
            else:
                print(f"Error adding batch: {data}")
        except Exception as e:
            print(f"Request failed: {e}")

def main():
    if not all([FEISHU_APP_ID, FEISHU_APP_SECRET, FEISHU_BASE_TOKEN, FEISHU_TABLE_ID, RSS_FEEDS_ENV]):
        print("Error: Missing environment variables.")
        return
    
    print("Starting sync job...")
    try:
        token = get_tenant_access_token()
        print("Authenticated with Feishu.")
    except Exception as e:
        print(f"Auth failed: {e}")
        return

    print("Fetching existing records...")
    existing_links = fetch_existing_records(token)
    print(f"Found {len(existing_links)} existing records.")
    
    feeds = [u.strip() for u in RSS_FEEDS_ENV.split(",") if u.strip()]
    new_records = parse_feeds(feeds, existing_links)
    
    if new_records:
        print(f"Found {len(new_records)} new articles. Syncing...")
        batch_create_records(token, new_records)
    else:
        print("No new articles found.")

if __name__ == "__main__":
    main()

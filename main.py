import os
import time
import requests
import feedparser
from dateutil import parser
from bs4 import BeautifulSoup

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
    # 获取现有记录以进行去重 (基于 Link 字段)
    url = f"{feishu_api_base}/bitable/v1/apps/{FEISHU_BASE_TOKEN}/tables/{FEISHU_TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    existing_links = set()
    page_token = None
    
    while True:
        params = {"page_size": 100}
        if page_token: params["page_token"] = page_token
        resp = requests.get(url, headers=headers, params=params).json()
        
        if "data" in resp and "items" in resp["data"]:
            for item in resp["data"]["items"]:
                # 如果是文本字段直接取，如果是超链接字段取 link 属性
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
                if link in existing_links: continue # 跳过已存在的
                
                existing_links.add(link)
                
                # 提取摘要（去除 HTML 标签）
                summary_raw = entry.get("summary", "") or entry.get("description", "")
                summary_text = BeautifulSoup(summary_raw, "html.parser").get_text()[:1000]
                
                # 处理时间
                pub_parsed = entry.get("published_parsed") or entry.get("updated_parsed")
                pub_ts = int(time.mktime(pub_parsed) * 1000) if pub_parsed else int(time.time() * 1000)

                record = {
                    "fields": {
                        "Title": entry.get("title", "No Title"),
                        "Link": {"text": "点击阅读", "link": link}, # 超链接格式
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

def batch_create(token, records):
    if not records: return
    url = f"{feishu_api_base}/bitable/v1/apps/{FEISHU_BASE_TOKEN}/tables/{FEISHU_TABLE_ID}/records/batch_create"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    # 飞书限制每次批量最多 100 条
    for i in range(0, len(records), 100):
        batch = records[i:i+100]
        requests.post(url, headers=headers, json={"records": batch})
        print(f"Synced {len(batch)} records.")

def main():
    if not all([FEISHU_APP_ID, FEISHU_APP_SECRET, FEISHU_BASE_TOKEN]):
        print("Missing config.")
        return
    
    token = get_tenant_access_token()
    existing = fetch_existing_records(token)
    print(f"Found {len(existing)} existing records.")
    
    feeds = [u.strip() for u in RSS_FEEDS_ENV.split(",") if u.strip()]
    new_recs = parse_feeds(feeds, existing)
    
    if new_recs:
        print(f"Found {len(new_recs)} new articles.")
        batch_create(token, new_recs)
    else:
        print("No new articles.")

if __name__ == "__main__":
    main()

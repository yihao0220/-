import os
import time
import requests
import feedparser
from dateutil import parser
from bs4 import BeautifulSoup
import json
import google.generativeai as genai
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Configuration ---
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
FEISHU_BASE_TOKEN = os.environ.get("FEISHU_BASE_TOKEN")
FEISHU_TABLE_ID = os.environ.get("FEISHU_TABLE_ID")
RSS_FEEDS_ENV = os.environ.get("RSS_FEEDS", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
FEISHU_WEBHOOK_URL = os.environ.get("FEISHU_WEBHOOK_URL")

# --- Constants ---
feishu_api_base = "https://open.feishu.cn/open-apis"

# --- Gemini Setup ---
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-2.0-flash') 
else:
    model = None

# --- Helper Functions ---

def create_session_with_retries():
    """创建一个带有重试机制的 session"""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def send_feishu_notification(message):
    """发送飞书机器人通知"""
    if not FEISHU_WEBHOOK_URL:
        # print("Warning: FEISHU_WEBHOOK_URL not configured.")
        return

    payload = {
        "msg_type": "text",
        "content": {
            "text": message
        }
    }
    try:
        requests.post(FEISHU_WEBHOOK_URL, json=payload)
    except Exception as e:
        print(f"Failed to send notification: {e}")

def get_tenant_access_token():
    url = f"{feishu_api_base}/auth/v3/tenant_access_token/internal"
    payload = {"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
    session = create_session_with_retries()
    try:
        response = session.post(url, json=payload)
        response.raise_for_status()
        return response.json()["tenant_access_token"]
    except Exception as e:
        raise Exception(f"Failed to get access token: {e}")

def summarize_with_gemini(text):
    if not model or not text: return ""
    try:
        # Prompt: 简洁、直接
        prompt = f"请用一句话概括这篇微信公众号文章的核心内容，直接输出结论，不要废话，50字以内：\n\n{text}"
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"Gemini Error: {e}")
        return ""

def clean_html(html_content):
    if not html_content: return ""
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator=' ', strip=True)
    return text[:10000]

def fetch_existing_records(token):
    url = f"{feishu_api_base}/bitable/v1/apps/{FEISHU_BASE_TOKEN}/tables/{FEISHU_TABLE_ID}/records"
    headers = {"Authorization": f"Bearer {token}"}
    existing_links = set()
    page_token = None
    session = create_session_with_retries()

    while True:
        params = {"page_size": 100}
        if page_token: params["page_token"] = page_token
        try:
            resp = session.get(url, headers=headers, params=params).json()
            if resp.get("code") != 0:
                print(f"Error fetching records: {resp}")
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
        except Exception as e:
            print(f"Fetch request failed: {e}")
            break
    return existing_links

def parse_feeds(feed_urls, existing_links):
    new_records = []
    session = create_session_with_retries()
    
    # Debug: 检查 Key 是否读取到 (只打印前几位，防泄漏)
    if GEMINI_API_KEY:
        print(f"DEBUG: GEMINI_API_KEY found (starts with {GEMINI_API_KEY[:4]}...)")
    else:
        print("DEBUG: GEMINI_API_KEY NOT FOUND!")

    for url in feed_urls:
        if not url.strip(): continue
        try:
            print(f"Checking feed: {url}")
            # 使用带有重试的 session 获取 feed 内容
            response = session.get(url, timeout=30)
            feed = feedparser.parse(response.content)
            
            if feed.bozo:
                print(f"Warning: Feed {url} might be malformed: {feed.bozo_exception}")

            feed_title = feed.feed.get("title", "Unknown Source")
            
            # 处理所有文章
            entries = feed.entries
            print(f"DEBUG: Found {len(entries)} entries in feed.")
            
            for entry in entries:
                link = entry.get("link", "")
                if link in existing_links: continue 
                
                existing_links.add(link)
                
                # 获取内容
                content_raw = ""
                # 优先尝试获取 content (WeWe-RSS 通常有 content 字段)
                if hasattr(entry, 'content'):
                    content_raw = entry.content[0].value
                else:
                    content_raw = entry.get("summary", "") or entry.get("description", "")
                
                clean_text = clean_html(content_raw)
                
                ai_summary = ""
                # 只有当有 Key 且字数够才总结
                if GEMINI_API_KEY and len(clean_text) > 50:
                    print(f"Summarizing: {entry.get('title', 'No Title')}...")
                    ai_summary = summarize_with_gemini(clean_text)
                    if not ai_summary:
                        print("DEBUG: AI summary failed, using fallback.")
                        ai_summary = clean_text[:100] + "..."
                else:
                    if not GEMINI_API_KEY:
                        print("DEBUG: Skipping AI (No Key)")
                    elif len(clean_text) <= 50:
                        print(f"DEBUG: Skipping AI (Text too short: {len(clean_text)})")
                    ai_summary = clean_text[:100] + "..."

                pub_parsed = entry.get("published_parsed") or entry.get("updated_parsed")
                pub_ts = int(time.mktime(pub_parsed) * 1000) if pub_parsed else int(time.time() * 1000)

                record = {
                    "fields": {
                        "Title": entry.get("title", "No Title"),
                        "Link": {"text": "点击阅读", "link": link},
                        "Source": feed_title,
                        "Author": entry.get("author", feed_title),
                        "Summary": ai_summary,
                        "Date": pub_ts
                    }
                }
                new_records.append(record)
                
        except Exception as e:
            error_msg = f"Error parsing feed {url}: {e}"
            print(error_msg)
            send_feishu_notification(f"⚠️ {error_msg}")

    return new_records

def batch_create_records(token, records):
    if not records: return
    url = f"{feishu_api_base}/bitable/v1/apps/{FEISHU_BASE_TOKEN}/tables/{FEISHU_TABLE_ID}/records/batch_create"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    batch_size = 100
    session = create_session_with_retries()
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        payload = {"records": batch}
        try:
            print(f"Sending batch of {len(batch)} records...")
            response = session.post(url, headers=headers, json=payload)
            data = response.json()
            
            if data.get("code") == 0:
                print(f"Successfully added {len(batch)} records.")
            else:
                error_msg = f"Error adding batch: {data}"
                print(error_msg)
                send_feishu_notification(f"⚠️ {error_msg}")
        except Exception as e:
            error_msg = f"Request failed: {e}"
            print(error_msg)
            send_feishu_notification(f"⚠️ {error_msg}")

def main():
    if not all([FEISHU_APP_ID, FEISHU_APP_SECRET, FEISHU_BASE_TOKEN, FEISHU_TABLE_ID, RSS_FEEDS_ENV]):
        print("Error: Missing environment variables.")
        send_feishu_notification("⚠️ GitHub Action 缺少环境变量配置，请检查 Secrets。")
        return
    
    print("Starting sync job...")
    try:
        token = get_tenant_access_token()
        print("Authenticated with Feishu.")
    except Exception as e:
        error_msg = f"Auth failed: {e}"
        print(error_msg)
        send_feishu_notification(f"⚠️ {error_msg}")
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
    
    print("Job finished.")

if __name__ == "__main__":
    main()

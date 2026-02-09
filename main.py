import os
import time
import requests
import feedparser
from dateutil import parser
from bs4 import BeautifulSoup
import json
import google.generativeai as genai
import re

# --- Configuration ---
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
FEISHU_BASE_TOKEN = os.environ.get("FEISHU_BASE_TOKEN")
FEISHU_TABLE_ID = os.environ.get("FEISHU_TABLE_ID")
RSS_FEEDS_ENV = os.environ.get("RSS_FEEDS", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") # 新增

# --- Constants ---
feishu_api_base = "https://open.feishu.cn/open-apis"

# --- Gemini Setup ---
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    # 使用 Gemini 2.0 Flash (速度快，适合总结)
    model = genai.GenerativeModel('gemini-2.0-flash') 
else:
    model = None

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
        except Exception:
            break
            
        if "data" in resp and "items" in resp["data"]:
            for item in resp["data"]["items"]:
                link_field = item["fields"].get("Link", "")
                if isinstance(link_field, dict):
                    existing_links.add(link_field.get("link", ""))
                else:
                    existing_links.add(link_field)
        
        if resp.get("data", {}).get("has_more"):
            page_token = resp["data"]["page_token"]
        else:
            break
    return existing_links

def clean_html(html_content):
    """清洗 HTML，提取纯文本，用于发给 AI"""
    if not html_content: return ""
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator=' ', strip=True)
    return text[:10000] # 截取前 10000 字，防止超 token

def summarize_with_gemini(text):
    """调用 Gemini 生成一句话总结"""
    if not model or not text: return ""
    
    try:
        # Prompt 设计：简洁、直接
        prompt = f"请用一句话概括这篇微信公众号文章的核心内容，直接输出结论，不要废话，50字以内：\n\n{text}"
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"Gemini Error: {e}")
        return "" # 失败则返回空字符串

def parse_feeds(feed_urls, existing_links):
    new_records = []
    for url in feed_urls:
        if not url.strip(): continue
        try:
            print(f"Checking feed: {url}")
            feed = feedparser.parse(url)
            feed_title = feed.feed.get("title", "Unknown Source")
            
            # 为了省钱省时间，每次只处理最新的 5 篇文章 (可调)
            # entries = feed.entries[:5] 
            entries = feed.entries 

            for entry in entries:
                link = entry.get("link", "")
                if link in existing_links: continue 
                
                existing_links.add(link)
                
                # 1. 尝试获取全文 (content > summary > description)
                content_raw = ""
                if 'content' in entry:
                    content_raw = entry.content[0].value
                else:
                    content_raw = entry.get("summary", "") or entry.get("description", "")
                
                # 2. 清洗文本
                clean_text = clean_html(content_raw)
                
                # 3. AI 总结 (如果是新文章)
                ai_summary = ""
                if GEMINI_API_KEY and len(clean_text) > 50: # 太短的不总结
                    print(f"Summarizing: {entry.get('title', 'No Title')}...")
                    ai_summary = summarize_with_gemini(clean_text)
                    # 如果 AI 总结失败或为空，回退到截取前 100 字
                    if not ai_summary:
                        ai_summary = clean_text[:100] + "..."
                else:
                     # 没有 Key 或者文章太短，直接截取
                    ai_summary = clean_text[:100] + "..."

                # 处理时间
                pub_parsed = entry.get("published_parsed") or entry.get("updated_parsed")
                pub_ts = int(time.mktime(pub_parsed) * 1000) if pub_parsed else int(time.time() * 1000)

                record = {
                    "fields": {
                        "Title": entry.get("title", "No Title"),
                        "Link": {"text": "点击阅读", "link": link},
                        "Source": feed_title,
                        "Author": entry.get("author", feed_title),
                        "Summary": ai_summary, # 这里填入 AI 总结
                        "Date": pub_ts
                    }
                }
                new_records.append(record)
                
        except Exception as e:
            print(f"Error parsing {url}: {e}")
    return new_records

def batch_create_records(token, records):
    if not records: return
    url = f"{feishu_api_base}/bitable/v1/apps/{FEISHU_BASE_TOKEN}/tables/{FEISHU_TABLE_ID}/records/batch_create"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    batch_size = 100
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        payload = {"records": batch}
        try:
            print(f"Sending batch of {len(batch)} records...")
            requests.post(url, headers=headers, json=payload)
            print(f"Successfully added {len(batch)} records.")
        except Exception as e:
            print(f"Request failed: {e}")

def main():
    if not all([FEISHU_APP_ID, FEISHU_APP_SECRET, FEISHU_BASE_TOKEN, FEISHU_TABLE_ID, RSS_FEEDS_ENV]):
        print("Error: Missing environment variables.")
        return
    
    token = get_tenant_access_token()
    existing_links = fetch_existing_records(token)
    print(f"Found {len(existing_links)} existing records.")
    
    feeds = [u.strip() for u in RSS_FEEDS_ENV.split(",") if u.strip()]
    new_records = parse_feeds(feeds, existing_links)
    
    if new_records:
        batch_create_records(token, new_records)
    else:
        print("No new articles found.")

if __name__ == "__main__":
    main()

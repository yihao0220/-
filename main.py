import os
import time
import requests
import feedparser
from dateutil import parser
from bs4 import BeautifulSoup
import json
import google.generativeai as genai
import re
import random
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
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-2.0-flash')
    except Exception as e:
        print(f"Gemini Init Error: {e}")
        model = None
else:
    model = None

# --- Helper Functions ---

def create_session_with_retries():
    """创建一个带有重试机制和伪装头的 session"""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    })
    return session

def send_feishu_notification(message):
    """发送报错通知"""
    if not FEISHU_WEBHOOK_URL: return
    try:
        requests.post(FEISHU_WEBHOOK_URL, json={"msg_type": "text", "content": {"text": message}})
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
        error_msg = f"Failed to get access token: {e}"
        print(error_msg)
        send_feishu_notification(f"⚠️ {error_msg}")
        raise e

def summarize_with_gemini(text):
    if not model or not text: return ""
    try:
        # 截取前 8000 字防止超 Token
        prompt = f"请用一句话概括这篇微信公众号文章的核心内容，直接输出结论，不要废话，50字以内：\n\n{text[:8000]}"
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"Gemini Error: {e}")
        return ""

def create_feishu_doc(token, title, content):
    """创建飞书文档并写入内容"""
    print(f"Creating Feishu Doc: {title}...")
    session = create_session_with_retries()
    
    # 1. 创建空文档
    create_url = f"{feishu_api_base}/docx/v1/documents"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"title": title}
    
    try:
        resp = session.post(create_url, headers=headers, json=payload)
        resp_json = resp.json()
        if resp_json.get("code") != 0:
            print(f"Error creating doc: {resp_json}")
            return ""
        
        doc_id = resp_json["data"]["document"]["document_id"]
        
        # 2. 写入内容
        blocks_url = f"{feishu_api_base}/docx/v1/documents/{doc_id}/blocks/children"
        
        # 简单清洗内容
        clean_content = re.sub(r'\n\s*\n', '\n\n', content)
        # 分段写入
        text_chunks = [clean_content[i:i+3000] for i in range(0, len(clean_content), 3000)]
        
        children = []
        for chunk in text_chunks:
            children.append({
                "block_type": 2, # Text block
                "text": {"elements": [{"text_run": {"content": chunk}}]}
            })
            
        block_payload = {"children": children}
        session.post(blocks_url, headers=headers, json=block_payload)
        
        doc_url = f"https://feishu.cn/docx/{doc_id}"
        print(f"Doc created: {doc_url}")
        return doc_url
        
    except Exception as e:
        print(f"Failed to create doc: {e}")
        return ""

def fetch_wechat_content(url):
    """爬虫：抓取微信文章正文"""
    session = create_session_with_retries()
    try:
        # 随机延迟，模拟人类
        time.sleep(random.uniform(1, 3))
        response = session.get(url, timeout=10)
        soup = BeautifulSoup(response.content, "html.parser")
        
        # 微信文章正文在 js_content
        content_div = soup.find("div", id="js_content")
        if content_div:
            return content_div.get_text(separator='\n', strip=True)
        
        # 兜底：获取全文
        return soup.get_text(separator='\n', strip=True)
    except Exception as e:
        print(f"Failed to fetch url {url}: {e}")
        return ""

def clean_html_simple(html):
    if not html: return ""
    return BeautifulSoup(html, "html.parser").get_text(separator='\n', strip=True)

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
            if resp.get("code") != 0: break
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
        except Exception:
            break
    return existing_links

def parse_feeds(feed_urls, existing_links, token):
    new_records = []
    session = create_session_with_retries()
    
    if GEMINI_API_KEY:
        print(f"DEBUG: GEMINI_API_KEY configured.")

    for url in feed_urls:
        if not url.strip(): continue
        try:
            print(f"Checking feed: {url}")
            response = session.get(url, timeout=30)
            feed = feedparser.parse(response.content)
            
            for entry in feed.entries:
                link = entry.get("link", "")
                # 去重
                if link in existing_links: continue 
                
                existing_links.add(link)
                title = entry.get("title", "No Title")
                
                # --- 1. 获取正文 (核心逻辑) ---
                content_text = ""
                
                # A. 优先从 RSS 获取
                if hasattr(entry, 'content'):
                    content_text = clean_html_simple(entry.content[0].value)
                else:
                    content_text = clean_html_simple(entry.get("summary", "") or entry.get("description", ""))
                
                # B. 如果 RSS 内容太短 (少于 100 字) 且有链接，启用爬虫抓取原文
                if len(content_text) < 100 and link:
                    print(f"DEBUG: Content too short ({len(content_text)}). Fetching original: {title}")
                    fetched_text = fetch_wechat_content(link)
                    if len(fetched_text) > len(content_text):
                        content_text = fetched_text
                
                # --- 2. 创建飞书文档 (新功能) ---
                doc_link = ""
                # 只有内容稍微丰富点才创建文档，避免空文档
                if len(content_text) > 50:
                    doc_link = create_feishu_doc(token, title, content_text)
                
                # --- 3. AI 总结 ---
                ai_summary = ""
                if GEMINI_API_KEY and len(content_text) > 50:
                    print(f"Summarizing: {title}...")
                    ai_summary = summarize_with_gemini(content_text)
                    if not ai_summary:
                        ai_summary = content_text[:100] + "..."
                else:
                    ai_summary = content_text[:100] + "..."

                pub_parsed = entry.get("published_parsed") or entry.get("updated_parsed")
                pub_ts = int(time.mktime(pub_parsed) * 1000) if pub_parsed else int(time.time() * 1000)

                # --- 4. 组装数据 ---
                fields = {
                    "Title": title,
                    "Link": {"text": "原文链接", "link": link},
                    "Source": feed.feed.get("title", "Unknown Source"),
                    "Author": entry.get("author", "Unknown"),
                    "Summary": ai_summary,
                    "Date": pub_ts
                }
                
                # 如果生成了文档，填入 Doc Link
                if doc_link:
                    fields["Doc Link"] = {"text": "飞书文档", "link": doc_link}

                new_records.append({"fields": fields})
                
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
    # 检查 Secrets
    if not all([FEISHU_APP_ID, FEISHU_APP_SECRET, FEISHU_BASE_TOKEN, FEISHU_TABLE_ID, RSS_FEEDS_ENV]):
        print("Error: Missing environment variables.")
        send_feishu_notification("⚠️ GitHub Action 缺少环境变量配置，请检查 Secrets。")
        return
    
    print("Starting sync job...")
    try:
        token = get_tenant_access_token()
        print("Authenticated with Feishu.")
    except Exception as e:
        # Auth 失败已在 get_tenant_access_token 里发送通知
        return

    print("Fetching existing records...")
    existing_links = fetch_existing_records(token)
    print(f"Found {len(existing_links)} existing records.")
    
    feeds = [u.strip() for u in RSS_FEEDS_ENV.split(",") if u.strip()]
    
    # 开始处理 (传入 token 以便创建文档)
    new_records = parse_feeds(feeds, existing_links, token)
    
    if new_records:
        print(f"Found {len(new_records)} new articles. Syncing...")
        batch_create_records(token, new_records)
    else:
        print("No new articles found.")
    
    print("Job finished.")

if __name__ == "__main__":
    main()

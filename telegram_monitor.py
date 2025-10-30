import asyncio
import threading
from datetime import datetime
import requests
import time
import hmac
import hashlib
import base64
import urllib.parse
from urllib.parse import urlparse
from telethon import TelegramClient, events
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from database import Config, MonitoredGroup, Keyword, MatchedMessage, DB_URI

client_instance = None
client_thread = None
is_running = False
main_loop = None
client_ready = threading.Event()

def get_db_session():
    engine = create_engine(DB_URI)
    Session = sessionmaker(bind=engine)
    return Session()

def is_safe_url(url):
    try:
        parsed_url = urlparse(url)
        if parsed_url.scheme not in ['http', 'https']:
            return False
        # 限制为钉钉的官方域名
        allowed_domains = ['oapi.dingtalk.com']
        if parsed_url.netloc not in allowed_domains:
            return False
        return True
    except Exception:
        return False

def send_to_dingtalk(webhook_url, secret, title, message, is_test=False):
    if not webhook_url:
        if is_test: return "钉钉Webhook未配置。"
        print("钉钉Webhook未配置，跳过发送。")
        return
    
    if not is_safe_url(webhook_url):
        error_msg = f"检测到不安全的Webhook URL: {webhook_url}"
        print(error_msg)
        if is_test: return error_msg
        return

    if secret:
        timestamp = str(round(time.time() * 1000))
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        webhook_url = f"{webhook_url}&timestamp={timestamp}&sign={sign}"

    headers = {'Content-Type': 'application/json;charset=utf-8'}
    data = {
        "msgtype": "markdown",
        "markdown": {
            "title": title,
            "text": message
        }
    }
    try:
        response = requests.post(webhook_url, headers=headers, json=data)
        if response.status_code == 200 and response.json().get("errcode") == 0:
            print("成功发送钉钉通知。")
            if is_test: return "测试消息发送成功！"
        else:
            print(f"发送钉钉通知失败: {response.text}")
            if is_test: return f"发送失败: {response.text}"
    except Exception as e:
        print(f"发送钉钉通知时发生异常: {e}")
        if is_test: return f"发生异常: {e}"

async def start_client_async(api_id, api_hash, phone_number):
    global client_instance, is_running
    
    session = get_db_session()
    client = TelegramClient('telegram_session', api_id, api_hash, system_version="4.16.30-vxCUSTOM")
    client_instance = client
        
    @client.on(events.NewMessage)
    async def handler(event):
        chat = await event.get_chat()
        print(f"[调试] 收到新消息, 来自群组: '{getattr(chat, 'title', '未知群组')}' (ID: {chat.id})")

        session_handler = get_db_session() 
        try:
            sender = await event.get_sender()

            group_name = getattr(chat, 'title', '未知群组')
            sender_name = None 
            if sender:
                sender_name = getattr(sender, 'username', None)
                if not sender_name: 
                    first_name = getattr(sender, 'first_name', '') or ''
                    last_name = getattr(sender, 'last_name', '') or ''
                    sender_name = f"{first_name} {last_name}".strip()
            
            if sender_name is None and hasattr(chat, 'title'):
                sender_name = chat.title

            standard_chat_id = str(chat.id)
            if standard_chat_id.startswith('-100'):
                standard_chat_id = standard_chat_id[4:]
            
            all_monitored_ids = [g.group_identifier for g in session_handler.query(MonitoredGroup).all()]
            
            standard_monitored_ids = []
            for mid in all_monitored_ids:
                if mid.startswith('-100'):
                    standard_monitored_ids.append(mid[4:])
                else:
                    standard_monitored_ids.append(mid)

            current_group_obj = None
            
            if standard_chat_id in standard_monitored_ids:
                original_id = None
                for mid in all_monitored_ids:
                    if mid.endswith(standard_chat_id):
                        original_id = mid
                        break
                if original_id:
                    current_group_obj = session_handler.query(MonitoredGroup).filter_by(group_identifier=original_id).first()

            if not current_group_obj and hasattr(chat, 'username') and chat.username in all_monitored_ids:
                current_group_obj = session_handler.query(MonitoredGroup).filter_by(group_identifier=chat.username).first()


            if current_group_obj:
                print(f"[调试] 群组 '{getattr(chat, 'title', '未知')}' 在监控列表中。开始检查关键词...")
                keywords_to_check = current_group_obj.keywords
                
                if not keywords_to_check:
                    print(f"[调试] 注意: 群组 '{getattr(chat, 'title', '未知')}' 没有配置任何关键词。")
                else:
                    keyword_texts = [k.text for k in keywords_to_check]
                    print(f"[调试] 为该群组配置的关键词: {keyword_texts}")

                    for keyword in keywords_to_check:
                        if keyword.text.lower() in event.message.message.lower():
                            print(f"[调试] 成功! 在消息中找到关键词 '{keyword.text}'。")
                            new_message = MatchedMessage(
                                group_name=group_name,
                                message_content=event.message.message,
                                sender=sender_name,
                                message_date=datetime.now(),
                                matched_keyword=keyword.text
                            )
                            session_handler.add(new_message)
                            session_handler.commit()
                            print(f"在群组 '{group_name}' 中匹配到关键词 '{keyword.text}'")

                            config = session_handler.query(Config).first()
                            if config and config.dingtalk_webhook:
                                title = f"关键词 '{keyword.text}' 触发"
                                ding_message = (
                                    f"#### **关键词监控提醒**\n\n"
                                    f"> **群组**: {group_name}\n\n"
                                    f"> **发送人**: {sender_name or 'N/A'}\n\n"
                                    f"> **关键词**: {keyword.text}\n\n"
                                    f"> **消息内容**: {event.message.message}\n"
                                )
                                send_to_dingtalk(config.dingtalk_webhook, config.dingtalk_secret, title, ding_message)
                            break 
            else:
                print(f"[调试] 群组 '{getattr(chat, 'title', '未知')}' (ID: {chat.id}) 不在监控列表中，已忽略。")
        finally:
            session_handler.close() 

    try:
        await client.connect()
        if not await client.is_user_authorized():
            await client.send_code_request(phone_number)
            try:
                await client.sign_in(phone_number, input('请输入telegram发来的验证码: '))
            except Exception:
                await client.sign_in(password=input('请输入两步验证密码: '))

        is_running = True
        print("Telegram客户端已成功连接并开始监听...")
        client_ready.set() 
        
        await client.run_until_disconnected()
    finally:
        if client_instance and client_instance.is_connected():
            await client_instance.disconnect()
        is_running = False
        client_instance = None
        print("Telegram客户端已断开连接。")
        session.close()

def run_in_thread(loop, coro):
    global main_loop
    main_loop = loop 
    asyncio.set_event_loop(loop)
    loop.run_until_complete(coro)



def start_monitoring():
    global client_thread, is_running, main_loop
    
    if client_thread and client_thread.is_alive():
        print("监控已经在运行中。")
        return
    
    client_ready.clear() 
    session = get_db_session()
    config = session.query(Config).first()
    session.close()

    if not (config and config.api_id and config.api_hash and config.phone_number):
        return
    
    loop = asyncio.new_event_loop()
    main_loop = loop
    
    coro = start_client_async(config.api_id, config.api_hash, config.phone_number)
    
    client_thread = threading.Thread(target=run_in_thread, args=(loop, coro))
    client_thread.daemon = True
    client_thread.start()

def stop_monitoring():
    global client_instance, is_running, client_thread, main_loop
    if not (client_thread and client_thread.is_alive()):
        return

    if main_loop and main_loop.is_running():
        main_loop.call_soon_threadsafe(
            lambda: asyncio.create_task(client_instance.disconnect())
        )
    
    client_thread.join(timeout=5)
    
    is_running = False
    main_loop = None
    client_thread = None

import os
import asyncio
from telethon import TelegramClient
from urllib.parse import urlparse
import concurrent.futures
import time
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.errors import (
    ChannelPrivateError, ChannelInvalidError, UserBannedInChannelError,
    ChannelsTooMuchError, UserAlreadyParticipantError
)

import telegram_monitor
import json
import csv
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from datetime import datetime
from database import get_session, ExportTask, MonitoredGroup

async def update_monitored_groups_info_async():
    client = telegram_monitor.client_instance
    if not (client and client.is_connected()):
        return {'error': '监控客户端未运行或未连接。'}

    db_session = get_session()
    try:
        monitored_groups = db_session.query(MonitoredGroup).all()
        if not monitored_groups:
            return {'updated_count': 0}

        updated_count = 0
        for group in monitored_groups:
            try:
                entity = await client.get_entity(int(group.group_identifier))
                
                # Update group name
                if entity.title != group.group_name:
                    group.group_name = entity.title
                    db_session.commit()

                # Update logo
                logo_dir = os.path.join(basedir, 'static', 'logos')
                os.makedirs(logo_dir, exist_ok=True)
                logo_filename = f"{entity.id}.jpg"
                logo_abs_path = os.path.join(logo_dir, logo_filename)
                
                path = await client.download_profile_photo(entity, file=logo_abs_path)
                if path:
                    logo_rel_path = f"logos/{logo_filename}"
                    if group.logo_path != logo_rel_path:
                        group.logo_path = logo_rel_path
                        db_session.commit()
                
                updated_count += 1
            except Exception as e:
                print(f"Could not update group {group.group_identifier}: {e}")
                continue
        
        return {'updated_count': updated_count}
    finally:
        db_session.close()

def update_monitored_groups_info():
    if not telegram_monitor.main_loop:
        return {'error': '事件循环未准备好。'}
    
    future = asyncio.run_coroutine_threadsafe(
        update_monitored_groups_info_async(), 
        telegram_monitor.main_loop
    )
    
    try:
        return future.result(timeout=60)
    except Exception as e:
        return {'error': f'执行时发生错误: {e}'}

basedir = os.path.abspath(os.path.dirname(__file__))

async def get_group_details_async(group_identifier):
    client = telegram_monitor.client_instance
    if not (client and client.is_connected()):
        return {'error': '监控客户端未运行或未连接。'}

    try:
        path = urlparse(group_identifier).path.strip('/')
        if '/' in path:
            identifier = path.split('/')[-1]
        else:
            identifier = path
            
        try:
            entity = await client.get_entity(identifier)
        except (ValueError, TypeError):
            return {'error': f"找不到群组 '{identifier}'。请检查链接或用户名。"}

        group_id = entity.id
        group_name = entity.title

        logo_dir = os.path.join(basedir, 'static', 'logos')
        os.makedirs(logo_dir, exist_ok=True)
        logo_filename = f"{group_id}.jpg"
        logo_abs_path = os.path.join(logo_dir, logo_filename)
        
        path = await client.download_profile_photo(entity, file=logo_abs_path)
        
        logo_rel_path = f"logos/{logo_filename}" if path else None

        return {
            'success': True,
            'identifier': str(group_id),
            'name': group_name,
            'logo_path': logo_rel_path
        }

    except Exception as e:
        return {'error': f'发生未知错误: {e}'}

def get_group_details(group_identifier):
    for _ in range(10): 
        if telegram_monitor.client_instance and telegram_monitor.client_instance.is_connected() and telegram_monitor.main_loop:
            break
        time.sleep(1)
    else:
        return {'error': '监控客户端未能成功连接或启动超时。请检查网络、API凭据或重启程序。'}
    
    current_loop = telegram_monitor.main_loop
    if not current_loop:
        return {'error': '事件循环丢失，这是一个严重错误，请重启程序。'}
    
    future = asyncio.run_coroutine_threadsafe(
        get_group_details_async(group_identifier), 
        current_loop
    )
    
    try:
        return future.result(timeout=20)
    except concurrent.futures.TimeoutError:
        return {'error': '操作超时，无法连接到Telegram。可能是网络问题。'}
    except Exception as e:
        return {'error': f'执行时发生错误: {e}'}

async def get_my_groups_async():
    client = telegram_monitor.client_instance
    if not (client and client.is_connected()):
        return {'error': '监控客户端未运行或未连接。'}

    groups = []
    try:
        all_dialogs = await client.get_dialogs()
        for dialog in all_dialogs:
            if dialog.is_group or dialog.is_channel:
                groups.append({
                    'id': str(dialog.id),
                    'name': dialog.name,
                    'logo_path': None
                })
        return {'success': True, 'groups': groups}
    except Exception as e:
        return {'error': f'获取群组列表时发生错误: {e}'}

def get_my_groups():
    if not telegram_monitor.main_loop:
        return {'error': '事件循环未准备好。'}
    
    future = asyncio.run_coroutine_threadsafe(
        get_my_groups_async(), 
        telegram_monitor.main_loop
    )
    
    try:
        return future.result(timeout=60)
    except Exception as e:
        return {'error': f'执行时发生错误: {e}'}

async def batch_join_groups_async(task_id, links, delay, tasks_dict, lock_obj):
    client = telegram_monitor.client_instance
    if not (client and client.is_connected()):
        with lock_obj:
            task = tasks_dict[task_id]
            task['status'] = 'error'
            task['log'].append('[ERROR] 监控客户端未连接，任务无法执行。')
        return

    total_links = len(links)
    with lock_obj:
        task = tasks_dict[task_id]
        task['status'] = 'running'
        task['log'].append(f'[INFO] 任务开始，共 {total_links} 个群组链接。')
        if total_links > 20:
            task['log'].append(f'[WARN] 本次任务包含 {total_links} 个群组，超过20个。请注意，单日大量加群可能会增加账户风险。')
        if delay < 20:
            task['log'].append(f'[WARN] 用户设置间隔低于安全阈值，已强制使用20秒间隔。')
            delay = 20


    for i, link in enumerate(links):
        with lock_obj:
            task = tasks_dict[task_id]
            if task['stop_requested']:
                task['log'].append('[INFO] 检测到停止信号，任务已中止。')
                task['status'] = 'stopped'
                break
            task['current'] = i + 1
            task['log'].append(f'[ATTEMPT] ({i+1}/{total_links}) 正在尝试加入: {link}')
        
        try:
            parsed_link = urlparse(link)
            identifier = parsed_link.path.strip('/').split('/')[-1]
            if not identifier:
                with lock_obj:
                    tasks_dict[task_id]['log'].append('[ERROR] 链接格式不正确，已跳过。')
                continue

            entity = await client.get_entity(identifier)
            await client(JoinChannelRequest(entity))
            
            with lock_obj:
                task = tasks_dict[task_id]
                task['log'].append(f'[SUCCESS] 成功加入群组: {getattr(entity, "title", identifier)}')

        except (ChannelPrivateError, UserBannedInChannelError):
            with lock_obj:
                tasks_dict[task_id]['log'].append('[ERROR] 加入失败：群组是私有的或您被禁止加入。')
        except ChannelsTooMuchError:
            with lock_obj:
                task = tasks_dict[task_id]
                task['log'].append('[ERROR] 加入失败：您已加入过多的群组或频道。任务已中止。')
                task['status'] = 'error'
            break 
        except UserAlreadyParticipantError:
             with lock_obj:
                tasks_dict[task_id]['log'].append('[INFO] 您已经在这个群组里了，跳过。')
        except (ValueError, TypeError, ChannelInvalidError):
             with lock_obj:
                tasks_dict[task_id]['log'].append(f'[ERROR] 找不到群组 "{identifier}"，请检查链接是否正确。')
        except Exception as e:
            with lock_obj:
                tasks_dict[task_id]['log'].append(f'[ERROR] 发生未知错误: {str(e)}')
        
        if i < total_links - 1:
            with lock_obj:
                tasks_dict[task_id]['log'].append(f'[WAIT] 暂停 {delay} 秒...')
            time.sleep(delay)

    with lock_obj:
        task = tasks_dict[task_id]
        if task['status'] == 'running': 
            task['status'] = 'completed'
            task['log'].append('[INFO] 所有链接已处理完毕，任务完成！')

def batch_join_groups(task_id, links, delay, tasks_dict, lock_obj):
    loop = telegram_monitor.main_loop
    if not loop:
        with lock_obj:
            task = tasks_dict[task_id]
            task['status'] = 'error'
            task['log'].append('[ERROR] 事件循环丢失，这是一个严重错误，请重启程序。')
        return
    
    future = asyncio.run_coroutine_threadsafe(
        batch_join_groups_async(task_id, links, delay, tasks_dict, lock_obj), 
        loop
    )
    
    try:
        future.result()
    except Exception as e:
        with lock_obj:
            task = tasks_dict[task_id]
            task['status'] = 'error'
            task['log'].append(f'[FATAL] 执行时发生致命错误: {e}')



import logging
logger = logging.getLogger(__name__)

async def export_messages_async(task_id, group_identifier, file_format, update_callback):
    logger.info(f"Starting export task {task_id} for group {group_identifier} in {file_format} format.")
    client = telegram_monitor.client_instance
    if not (client and client.is_connected()):
        logger.error(f"Export task {task_id} failed: Telegram client not connected.")
        update_callback(task_id, 'error', log_message='监控客户端未连接，任务无法执行。')
        return

    try:
        # 确保group_identifier是整数，因为get_entity对于ID最好使用整数
        try:
            numeric_id = int(group_identifier)
        except ValueError:
            numeric_id = group_identifier # 如果不是数字，就按原样使用（例如username）

        update_callback(task_id, 'running', log_message="正在获取群组信息...")
        entity = await client.get_entity(numeric_id)
        update_callback(task_id, 'running', log_message=f"成功连接到群组: {entity.title}")

        messages_data = []
        total_messages = 0
        
        async for message in client.iter_messages(entity):
            total_messages += 1
            sender = await message.get_sender()
            sender_name = "N/A"
            if sender:
                sender_name = getattr(sender, 'username', None) or f"{getattr(sender, 'first_name', '')} {getattr(sender, 'last_name', '')}".strip()

            message_text = message.text or ""
            if message.photo:
                message_text += f" [图片-{message.id}]"
            elif message.video:
                message_text += f" [视频-{message.id}]"

            message_info = {
                'message_id': message.id,
                'sender': sender_name,
                'text': message_text,
                'date': message.date.isoformat(),
                'reply_to_message_id': message.reply_to_msg_id
            }
            messages_data.append(message_info)

            if total_messages % 100 == 0:
                logger.debug(f"Task {task_id}: Processed {total_messages} messages.")
                update_callback(task_id, 'running', log_message=f'已处理 {total_messages} 条消息...')
                
                db_session = get_session()
                try:
                    task = db_session.get(ExportTask, task_id)
                    if task and task.status == 'stopped':
                        logger.info(f"Task {task_id} stopped by user.")
                        update_callback(task_id, 'stopped', log_message='任务被用户手动停止。')
                        db_session.close()
                        return
                finally:
                    db_session.close()

        logger.info(f"Task {task_id}: Finished iterating messages. Total: {total_messages}.")

        exports_dir = os.path.join(basedir, 'exports')
        os.makedirs(exports_dir, exist_ok=True)
        
        safe_group_name = "".join(c for c in entity.title if c.isalnum() or c in (' ', '_')).rstrip()
        filename = f"{safe_group_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{file_format}"
        file_path = os.path.join(exports_dir, filename)
        logger.info(f"Task {task_id}: Saving file to {file_path}")

        if file_format == 'json':
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(messages_data, f, ensure_ascii=False, indent=4)
        elif file_format == 'csv':
            with open(file_path, 'w', encoding='utf-8-sig', newline='') as f:
                if messages_data:
                    writer = csv.DictWriter(f, fieldnames=messages_data[0].keys())
                    writer.writeheader()
                    writer.writerows(messages_data)

        logger.info(f"Task {task_id}: File saved successfully.")
        update_callback(task_id, 'completed', file_path=file_path, log_message=f'导出完成！共导出 {total_messages} 条消息。文件已保存至: {file_path}')

    except Exception as e:
        logger.error(f"An error occurred in export task {task_id}: {e}", exc_info=True)
        update_callback(task_id, 'error', log_message=f'导出过程中发生错误: {e}')

def run_export_task(task_id, group_identifier, file_format, update_callback):
    loop = telegram_monitor.main_loop
    if not loop:
        update_callback(task_id, 'error', log_message='事件循环丢失，这是一个严重错误，请重启程序。')
        return
    
    asyncio.run_coroutine_threadsafe(
        export_messages_async(task_id, group_identifier, file_format, update_callback), 
        loop
    )

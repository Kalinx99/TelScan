import asyncio
import os
import time
import json
import logging
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import UserAlreadyParticipantError, FloodWaitError, InviteHashExpiredError, InviteHashInvalidError
from telethon.tl.types import Dialog

import telegram_monitor
<<<<<<< Updated upstream
from database import get_session, MonitoredGroup

basedir = os.path.abspath(os.path.dirname(__file__))
logger = logging.getLogger(__name__)

# Reconstructed functions

def get_group_details(group_identifier):
    """
    Fetches details for a given group identifier (link or ID).
    This is a reconstructed function.
    """
    client = telegram_monitor.client_instance
    if not (client and client.is_connected()):
        return {'error': 'Telegram client not connected.'}

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    async def get_details():
        try:
            entity = await client.get_entity(group_identifier)
            
            logo_path = None
            logo_dir = os.path.join(basedir, 'static', 'logos')
            os.makedirs(logo_dir, exist_ok=True)
            logo_filename = f"{entity.id}.jpg"
            logo_abs_path = os.path.join(logo_dir, logo_filename)

            if await client.download_profile_photo(entity, file=logo_abs_path):
                logo_path = f"logos/{logo_filename}"

            return {
                'identifier': str(entity.id),
                'name': entity.title,
                'logo_path': logo_path,
                'error': None
            }
        except Exception as e:
            return {'error': str(e)}

    if loop.is_running():
        future = asyncio.run_coroutine_threadsafe(get_details(), loop)
        return future.result()
    else:
        return loop.run_until_complete(get_details())

def get_my_groups():
    """
    Fetches all groups the user is a member of.
    This is a reconstructed function.
    """
    client = telegram_monitor.client_instance
    if not (client and client.is_connected()):
        return {'error': 'Telegram client not connected.'}

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    async def get_groups():
        groups = []
        try:
            async for dialog in client.iter_dialogs():
                if dialog.is_group or dialog.is_channel:
                    logo_path = None
                    logo_dir = os.path.join(basedir, 'static', 'logos')
                    os.makedirs(logo_dir, exist_ok=True)
                    logo_filename = f"{dialog.entity.id}.jpg"
                    logo_abs_path = os.path.join(logo_dir, logo_filename)
                    
                    if await client.download_profile_photo(dialog.entity, file=logo_abs_path):
                        logo_path = f"logos/{logo_filename}"

                    groups.append({
                        'id': str(dialog.entity.id),
                        'name': dialog.name,
                        'logo_path': logo_path
                    })
            return {'groups': groups, 'error': None}
        except Exception as e:
            return {'error': str(e)}

    if loop.is_running():
        future = asyncio.run_coroutine_threadsafe(get_groups(), loop)
        return future.result()
    else:
        return loop.run_until_complete(get_groups())

def batch_join_groups(task_id, links, delay, batch_join_tasks, tasks_lock):
    """
    Joins a list of groups with a delay.
    This is a reconstructed function.
    """
    client = telegram_monitor.client_instance
    if not (client and client.is_connected()):
        with tasks_lock:
            batch_join_tasks[task_id]['log'].append('[ERROR] Telegram client not connected.')
            batch_join_tasks[task_id]['status'] = 'error'
        return

    loop = telegram_monitor.main_loop
    if not loop:
        with tasks_lock:
            batch_join_tasks[task_id]['log'].append('[ERROR] Main event loop not available.')
            batch_join_tasks[task_id]['status'] = 'error'
        return

    async def join_group(link):
        try:
            if 't.me/+' in link or 'telegram.me/+' in link:
                hash_ = link.split('+')[-1]
                await client(ImportChatInviteRequest(hash_))
            else:
                await client(JoinChannelRequest(link))
            return f"Successfully joined {link}"
        except (UserAlreadyParticipantError, InviteHashExpiredError, InviteHashInvalidError) as e:
            return f"Could not join {link}: {type(e).__name__}"
        except FloodWaitError as e:
            return f"Flood wait for {e.seconds} seconds."
        except Exception as e:
            return f"Error joining {link}: {e}"

    def run_batch_join():
        with tasks_lock:
            batch_join_tasks[task_id]['status'] = 'running'
            batch_join_tasks[task_id]['log'].append('[INFO] Starting batch join process.')

        for i, link in enumerate(links):
            with tasks_lock:
                if batch_join_tasks[task_id].get('stop_requested'):
                    batch_join_tasks[task_id]['log'].append('[INFO] Stop requested. Aborting.')
                    batch_join_tasks[task_id]['status'] = 'stopped'
                    break
                batch_join_tasks[task_id]['current'] = i + 1

            future = asyncio.run_coroutine_threadsafe(join_group(link), loop)
            result = future.result()
            
            with tasks_lock:
                batch_join_tasks[task_id]['log'].append(f'[INFO] {result}')
            
            time.sleep(delay)

        with tasks_lock:
            if batch_join_tasks[task_id]['status'] == 'running':
                batch_join_tasks[task_id]['status'] = 'completed'
                batch_join_tasks[task_id]['log'].append('[INFO] Batch join process finished.')

    # This function is called from a thread, so we can just run it
    run_batch_join()


def run_export_task(task_id, group_identifier, file_format, update_export_task_in_db):
    """
    Exports messages from a group to a file.
    This is a reconstructed function.
    """
    client = telegram_monitor.client_instance
    if not (client and client.is_connected()):
        update_export_task_in_db(task_id, 'error', log_message='Telegram client not connected.')
        return

    loop = telegram_monitor.main_loop
    if not loop:
        update_export_task_in_db(task_id, 'error', log_message='Main event loop not available.')
        return

    async def export_messages():
        messages = []
        try:
            update_export_task_in_db(task_id, 'running', log_message='Starting message export.')
            entity = await client.get_entity(group_identifier)
            async for message in client.iter_messages(entity, limit=None):
                messages.append({
                    'id': message.id,
                    'date': message.date.isoformat(),
                    'sender_id': message.sender_id,
                    'text': message.text,
                })
            
            export_dir = os.path.join(basedir, 'exports')
            os.makedirs(export_dir, exist_ok=True)
            file_path = os.path.join(export_dir, f'{task_id}.{file_format}')

            with open(file_path, 'w', encoding='utf-8') as f:
                if file_format == 'json':
                    json.dump(messages, f, ensure_ascii=False, indent=4)
            
            update_export_task_in_db(task_id, 'completed', file_path=file_path, log_message=f'Export completed. {len(messages)} messages saved.')

        except Exception as e:
            update_export_task_in_db(task_id, 'error', log_message=f'An error occurred: {e}')

    future = asyncio.run_coroutine_threadsafe(export_messages(), loop)
    try:
        future.result()
    except Exception as e:
        update_export_task_in_db(task_id, 'error', log_message=f'An error occurred in future: {e}')


# Original function from the user
async def update_group_logos_async():
    """
    Iterates through all monitored groups in the database and updates their profile photos.
    """
    client = telegram_monitor.client_instance
    if not (client and client.is_connected()):
        print("LogoUpdater: Telegram client not connected. Skipping.")
        return

    db_session = get_session()
    try:
        monitored_groups = db_session.query(MonitoredGroup).all()
        if not monitored_groups:
            print("LogoUpdater: No monitored groups to update.")
            return

        print(f"LogoUpdater: Starting check for {len(monitored_groups)} groups.")
        updated_count = 0

        for group in monitored_groups:
            try:
                # Use the numeric ID if possible, otherwise the username string
                identifier = int(group.group_identifier)
            except ValueError:
                identifier = group.group_identifier

            try:
                entity = await client.get_entity(identifier)
                
                logo_dir = os.path.join(basedir, 'static', 'logos')
                os.makedirs(logo_dir, exist_ok=True)
                logo_filename = f"{entity.id}.jpg"
                logo_abs_path = os.path.join(logo_dir, logo_filename)
                
                # download_profile_photo returns None if there's no new photo
                path = await client.download_profile_photo(entity, file=logo_abs_path)
                
                if path:
                    logo_rel_path = f"logos/{logo_filename}"
                    if group.logo_path != logo_rel_path:
                        group.logo_path = logo_rel_path
                        db_session.commit()
                        print(f"LogoUpdater: Updated logo for group '{group.group_name}'.")
                        updated_count += 1
                
                # Also update group name if it has changed
                if entity.title != group.group_name:
                    print(f"LogoUpdater: Group name for '{group.group_name}' changed to '{entity.title}'. Updating.")
                    group.group_name = entity.title
                    db_session.commit()


            except Exception as e:
                print(f"LogoUpdater: Could not update logo for group '{group.group_name}' (ID: {group.group_identifier}). Reason: {e}")
                continue
        
        if updated_count > 0:
            print(f"LogoUpdater: Finished. Updated logos for {updated_count} groups.")
=======

# from app import batch_join_tasks, tasks_lock  <-- This line is removed from here

basedir = os.path.abspath(os.path.dirname(__file__))

async def get_group_details_async(group_identifier):
    client = telegram_monitor.client_instance
    if not (client and client.is_connected()):
        return {'error': '监控客户端未运行或未连接。'}

    try:
        path = urlparse(group_identifier).path.strip('/')
        if '/' in path:
            identifier = path.split('/')[-1]
>>>>>>> Stashed changes
        else:
            print("LogoUpdater: Finished. No new logos found.")

<<<<<<< Updated upstream
    finally:
        db_session.close()
=======
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
                logo_rel_path = None
                logo_dir = os.path.join(basedir, 'static', 'logos')
                os.makedirs(logo_dir, exist_ok=True)
                logo_filename = f"{dialog.id}.jpg"
                logo_abs_path = os.path.join(logo_dir, logo_filename)
                
                try:
                    path = await client.download_profile_photo(dialog.entity, file=logo_abs_path)
                    if path:
                        logo_rel_path = f"logos/{logo_filename}"
                except Exception:
                    pass 

                groups.append({
                    'id': str(dialog.id),
                    'name': dialog.name,
                    'logo_path': logo_rel_path
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
>>>>>>> Stashed changes

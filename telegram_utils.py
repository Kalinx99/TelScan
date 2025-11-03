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
        else:
            print("LogoUpdater: Finished. No new logos found.")

    finally:
        db_session.close()
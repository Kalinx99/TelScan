import os
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, make_response, g, send_file, send_from_directory
from sqlalchemy.exc import IntegrityError, OperationalError
from waitress import serve
from datetime import datetime, time, timedelta
import uuid
from threading import Thread, Lock
import socket
from werkzeug.security import generate_password_hash, check_password_hash
import secrets
from functools import wraps
import pandas as pd
from io import BytesIO
import csv
import logging

from database import db, Config, MonitoredGroup, Keyword, MatchedMessage, DB_URI, User, Session, ExportTask
from telegram_monitor import start_monitoring, stop_monitoring, is_running
from telegram_utils import get_group_details, get_my_groups, batch_join_groups, run_export_task

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'your_very_secret_key_here_please_change_me')

batch_join_tasks = {}
tasks_lock = Lock()

db.init_app(app)

with app.app_context():
    db.create_all()

# --- Export Task Management ---
export_tasks = {}
export_tasks_lock = Lock()

def update_export_task_in_db(task_id, status, file_path=None, log_message=None):
    with app.app_context():
        task = db.session.get(ExportTask, task_id)
        if task:
            task.status = status
            if file_path:
                task.file_path = file_path
            if log_message:
                task.log = (task.log + '\n' if task.log else '') + f'[{datetime.now()}]: {log_message}'
            db.session.commit()

# 用于检查用户会话，并实现60分钟过期和自动续期
def check_session_and_renew():
    session_id = request.cookies.get('session_id')
    if not session_id:
        return None

    user_session = Session.query.filter_by(id=session_id).first()
    if not user_session:
        return None

    # 检查会话是否过期
    if user_session.expiration_time < datetime.now():
        db.session.delete(user_session)
        db.session.commit()
        return None

    # 会话未过期，自动续期60分钟
    user_session.expiration_time = datetime.now() + timedelta(minutes=60)
    db.session.commit()

    return db.session.get(User, user_session.user_id)

@app.before_request
def load_logged_in_user():
    g.user = check_session_and_renew()

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if g.user is None:
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/')
@login_required
def index():
    return redirect(url_for('groups'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if g.user:
        return redirect(url_for('groups'))

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        user = User.query.filter_by(username=username).first()

        if user and check_password_hash(user.password_hash, password):
            session_id = str(uuid.uuid4())
            expiration_time = datetime.now() + timedelta(minutes=60)
            new_session = Session(id=session_id, user_id=user.id, expiration_time=expiration_time)
            db.session.add(new_session)
            db.session.commit()

            response = make_response(redirect(request.args.get('next') or url_for('groups')))
            response.set_cookie('session_id', session_id, httponly=True, expires=expiration_time)
            flash('登录成功！', 'success')
            return response
        else:
            flash('用户名或密码错误。', 'danger')
            return redirect(url_for('login', next=request.args.get('next')))
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    session_id = request.cookies.get('session_id')
    if session_id:
        user_session = Session.query.filter_by(id=session_id).first()
        if user_session:
            db.session.delete(user_session)
            db.session.commit()
    
    response = make_response(redirect(url_for('login')))
    response.set_cookie('session_id', '', expires=0)
    flash('您已成功注销。', 'info')
    return response

@app.route('/config', methods=['GET', 'POST'])
@login_required
def config():
    config_item = Config.query.first()
    if request.method == 'POST':
        api_id = request.form.get('api_id')
        api_hash = request.form.get('api_hash')
        phone_number = request.form.get('phone_number')
        dingtalk_webhook = request.form.get('dingtalk_webhook')
        dingtalk_secret = request.form.get('dingtalk_secret')

        if config_item:
            config_item.api_id = api_id
            config_item.api_hash = api_hash
            config_item.phone_number = phone_number
            config_item.dingtalk_webhook = dingtalk_webhook
            config_item.dingtalk_secret = dingtalk_secret
        else:
            config_item = Config(
                api_id=api_id,
                api_hash=api_hash,
                phone_number=phone_number,
                dingtalk_webhook=dingtalk_webhook,
                dingtalk_secret=dingtalk_secret
            )
            db.session.add(config_item)
        
        db.session.commit()
        flash('配置已成功保存！', 'success')
        return redirect(url_for('config'))

    if not config_item:
        config_item = {
            'api_id': '', 
            'api_hash': '', 
            'phone_number': '', 
            'dingtalk_webhook': '',
            'dingtalk_secret': ''
        }

    return render_template('config.html', config=config_item, is_running=is_running)

@app.route('/api/batch_join', methods=['POST'])
@login_required
def start_batch_join():
    data = request.get_json()
    links_text = data.get('links', '')
    delay = data.get('delay', 60)

    links = [link.strip() for link in links_text.splitlines() if link.strip()]
    if not links:
        return jsonify({'error': '链接列表不能为空。'}), 400
    try:
        delay = int(delay)
        if delay < 20:
            delay = 20
    except (ValueError, TypeError):
        delay = 60

    task_id = str(uuid.uuid4())
    
    with tasks_lock:
        batch_join_tasks[task_id] = {
            'status': 'pending',
            'log': ['[INFO] 任务已创建，正在等待后台线程启动...'],
            'stop_requested': False,
            'total': len(links),
            'current': 0
        }

    thread = Thread(target=batch_join_groups, args=(task_id, links, delay, batch_join_tasks, tasks_lock))
    thread.daemon = True
    thread.start()

    return jsonify({'task_id': task_id})

@app.route('/api/batch_join/status/<task_id>', methods=['GET'])
@login_required
def get_batch_join_status(task_id):
    with tasks_lock:
        task = batch_join_tasks.get(task_id)
    
    if not task:
        return jsonify({'error': '任务未找到'}), 404
    
    return jsonify(task)

@app.route('/api/batch_join/stop/<task_id>', methods=['POST'])
@login_required
def stop_batch_join(task_id):
    with tasks_lock:
        task = batch_join_tasks.get(task_id)
        if task and task['status'] == 'running':
            task['stop_requested'] = True
            task['log'].append('[INFO] 收到停止请求，将在当前操作完成后中止...')
            return jsonify({'message': '停止请求已发送。'})

    return jsonify({'error': '任务未找到或已结束'}), 404

@app.route('/status')
@login_required
def status():
    from telegram_monitor import client_thread
    is_alive = client_thread is not None and client_thread.is_alive()
    return jsonify({'is_running': is_alive})

@app.route('/control/test_dingtalk', methods=['POST'])
@login_required
def test_dingtalk():
    config = Config.query.first()
    if not config or not config.dingtalk_webhook:
        flash('请先保存钉钉Webhook地址。', 'warning')
        return redirect(url_for('config'))

    title = "测试消息"
    message = "这是一条来自Telegram监控系统的测试消息。"
    
    from telegram_monitor import send_to_dingtalk
    result = send_to_dingtalk(config.dingtalk_webhook, config.dingtalk_secret, title, message, is_test=True)
    
    flash(f'钉钉测试结果: {result}', 'info')
    return redirect(url_for('config'))

@app.route('/groups', methods=['GET', 'POST'])
@login_required
def groups():
    if request.method == 'POST':
        group_identifier_input = request.form.get('group_identifier')
        
        if group_identifier_input:
            try:
                details = get_group_details(group_identifier_input)

                if details.get('error'):
                    flash(f"添加失败: {details['error']}", 'danger')
                else:
                    new_group = MonitoredGroup(
                        group_identifier=details['identifier'], 
                        group_name=details['name'],
                        logo_path=details['logo_path']
                    )
                    db.session.add(new_group)
                    try:
                        db.session.commit()
                        flash(f"群组 '{details['name']}' 添加成功！", 'success')
                    except IntegrityError:
                        db.session.rollback()
                        flash(f"群组 '{details['name']}' 已经存在。", 'danger')
            except OperationalError as e:
                db.session.rollback()
                if "database is locked" in str(e).lower():
                    flash("系统正忙，请稍后重试。可能是后台正在进行Telegram操作。", 'warning')
                else:
                    flash(f"发生数据库错误: {e}", 'danger')
            except Exception as e:
                db.session.rollback()
                flash(f"发生未知错误: {e}", 'danger')
        
        return redirect(url_for('groups'))
    
    all_groups = MonitoredGroup.query.order_by(MonitoredGroup.group_name).all()
    return render_template('groups.html', groups=all_groups)

@app.route('/add_my_groups', methods=['GET'])
@login_required
def add_my_groups_page():
    return render_template('add_my_groups.html')

@app.route('/api/get_my_groups', methods=['GET'])
@login_required
def api_get_my_groups():
    try:
        monitored_ids = {g.group_identifier for g in MonitoredGroup.query.all()}
    finally:
        db.session.remove()

    result = get_my_groups()
    if result.get('error'):
        return jsonify({'error': result['error']}), 500
    
    my_groups = [g for g in result['groups'] if g['id'] not in monitored_ids]
    
    return jsonify({'groups': my_groups})

@app.route('/groups/batch_add', methods=['POST'])
@login_required
def batch_add_groups():
    groups_to_add = request.form.getlist('groups')
    added_count = 0
    skipped_count = 0
    for group_data in groups_to_add:
        parts = group_data.split('|||')
        if len(parts) != 3: continue

        group_id, group_name, logo_path = parts
        
        exists = MonitoredGroup.query.filter_by(group_identifier=group_id).first()
        if not exists:
            new_group = MonitoredGroup(
                group_identifier=group_id,
                group_name=group_name,
                logo_path=logo_path if logo_path != 'None' else None
            )
            db.session.add(new_group)
            added_count += 1
        else:
            skipped_count += 1
    
    if added_count > 0:
        db.session.commit()
        flash(f'成功添加 {added_count} 个新群组！', 'success')
    if skipped_count > 0:
        flash(f'跳过 {skipped_count} 个已存在的群组。', 'info')

    return redirect(url_for('groups'))

@app.route('/groups/delete/<int:group_id>')
@login_required
def delete_group(group_id):
    group_to_delete = MonitoredGroup.query.get_or_404(group_id)
    db.session.delete(group_to_delete)
    db.session.commit()
    flash('群组已删除。', 'info')
    return redirect(url_for('groups'))

@app.route('/keywords', methods=['GET', 'POST'])
@login_required
def keywords():
    if request.method == 'POST':
        keywords_text = request.form.get('keywords_text', '').strip()
        group_ids = request.form.getlist('groups')

        if not keywords_text:
            flash('关键词列表不能为空。', 'danger')
        elif not group_ids:
            flash('必须至少选择一个群组。', 'danger')
        else:
            keywords_list = [kw.strip() for kw in keywords_text.splitlines() if kw.strip()]
            added_count = 0
            skipped_count = 0
            groups = MonitoredGroup.query.filter(MonitoredGroup.id.in_(group_ids)).all()

            for keyword_text in keywords_list:
                existing_keyword = Keyword.query.filter_by(text=keyword_text).first()
                if existing_keyword:
                    skipped_count += 1
                else:
                    new_keyword = Keyword(text=keyword_text)
                    new_keyword.groups.extend(groups)
                    db.session.add(new_keyword)
                    added_count += 1
            
            if added_count > 0:
                db.session.commit()
                flash(f'成功添加 {added_count} 个新关键词！', 'success')
            
            if skipped_count > 0:
                flash(f'跳过了 {skipped_count} 个已存在的关键词。', 'info')

        return redirect(url_for('keywords'))

    all_keywords = Keyword.query.all()
    all_groups = MonitoredGroup.query.all()
    return render_template('keywords.html', keywords=all_keywords, groups=all_groups)

@app.route('/keywords/edit/<int:keyword_id>', methods=['GET', 'POST'])
@login_required
def edit_keyword(keyword_id):
    keyword_to_edit = Keyword.query.get_or_404(keyword_id)
    if request.method == 'POST':
        group_ids = request.form.getlist('groups')
        if not group_ids:
            flash('必须至少选择一个群组。', 'danger')
        else:
            groups = MonitoredGroup.query.filter(MonitoredGroup.id.in_(group_ids)).all()
            keyword_to_edit.groups = groups 
            db.session.commit()
            flash('关键词关联已更新！', 'success')
        return redirect(url_for('keywords'))

    all_groups = MonitoredGroup.query.all()
    linked_group_ids = {group.id for group in keyword_to_edit.groups}
    return render_template('edit_keyword.html', keyword=keyword_to_edit, groups=all_groups, linked_group_ids=linked_group_ids)

@app.route('/keywords/delete/<int:keyword_id>')
@login_required
def delete_keyword(keyword_id):
    keyword_to_delete = Keyword.query.get_or_404(keyword_id)
    db.session.delete(keyword_to_delete)
    db.session.commit()
    flash('关键词已删除。', 'info')
    return redirect(url_for('keywords'))

@app.route('/messages')
@login_required
def messages():
    group_filter = request.args.get('group_name', '')
    start_date_filter = request.args.get('start_date', '')
    end_date_filter = request.args.get('end_date', '')
    keyword_filter = request.args.get('keyword', '')

    query = MatchedMessage.query

    if group_filter:
        query = query.filter(MatchedMessage.group_name == group_filter)
    if keyword_filter:
        query = query.filter(MatchedMessage.message_content.ilike(f'%{keyword_filter}%'))
    if start_date_filter:
        try:
            start_date = datetime.strptime(start_date_filter, '%Y-%m-%d').date()
            query = query.filter(MatchedMessage.message_date >= start_date)
        except ValueError:
            flash('无效的开始日期格式，请使用 YYYY-MM-DD。', 'danger')
    if end_date_filter:
        try:
            end_date = datetime.strptime(end_date_filter, '%Y-%m-%d').date()
            end_of_day = datetime.combine(end_date, time.max)
            query = query.filter(MatchedMessage.message_date <= end_of_day)
        except ValueError:
            flash('无效的结束日期格式，请使用 YYYY-MM-DD。', 'danger')

    try:
        matched_messages = query.order_by(MatchedMessage.message_date.desc()).all()
    except Exception as e:
        logger.error(f"Error querying messages: {str(e)}")
        flash(f'查询消息时发生错误: {str(e)}', 'danger')
        return render_template('messages.html', messages=[], group_logo_map={}, unique_group_names=[], filter_values={})

    all_message_groups = db.session.query(MatchedMessage.group_name).distinct().order_by(MatchedMessage.group_name).all()
    unique_group_names = [name for name, in all_message_groups]

    all_groups = MonitoredGroup.query.all()
    group_logo_map = {g.group_name: g.logo_path for g in all_groups}
    
    filter_values = {
        'group_name': group_filter,
        'start_date': start_date_filter,
        'end_date': end_date_filter,
        'keyword': keyword_filter
    }
    
    return render_template(
        'messages.html', 
        messages=matched_messages, 
        group_logo_map=group_logo_map,
        unique_group_names=unique_group_names,
        filter_values=filter_values
    )

@app.route('/export_messages', methods=['GET'])
@login_required
def export_messages():
    group_filter = request.args.get('group_name', '')
    start_date_filter = request.args.get('start_date', '')
    end_date_filter = request.args.get('end_date', '')
    keyword_filter = request.args.get('keyword', '')

    query = MatchedMessage.query

    if group_filter:
        query = query.filter(MatchedMessage.group_name == group_filter)
    if keyword_filter:
        query = query.filter(MatchedMessage.message_content.ilike(f'%{keyword_filter}%'))
    if start_date_filter:
        try:
            start_date = datetime.strptime(start_date_filter, '%Y-%m-%d').date()
            query = query.filter(MatchedMessage.message_date >= start_date)
        except ValueError:
            flash('无效的开始日期格式，请使用 YYYY-MM-DD。', 'danger')
            logger.error(f"Invalid start_date format: {start_date_filter}")
            return redirect(url_for('messages'))
    if end_date_filter:
        try:
            end_date = datetime.strptime(end_date_filter, '%Y-%m-%d').date()
            end_of_day = datetime.combine(end_date, time.max)
            query = query.filter(MatchedMessage.message_date <= end_of_day)
        except ValueError:
            flash('无效的结束日期格式，请使用 YYYY-MM-DD。', 'danger')
            logger.error(f"Invalid end_date format: {end_date_filter}")
            return redirect(url_for('messages'))

    try:
        matched_messages = query.order_by(MatchedMessage.message_date.desc()).all()
    except Exception as e:
        logger.error(f"Error querying messages for export: {str(e)}")
        flash(f'查询消息时发生错误: {str(e)}', 'danger')
        return redirect(url_for('messages'))

    try:
        # 转换为 pandas DataFrame
        data = [{
            'message_date': msg.message_date.strftime('%Y-%m-%d %H:%M:%S') if msg.message_date else '',
            'sender': msg.sender or '(未知来源)',
            'group_name': msg.group_name or '',
            'matched_keyword': msg.matched_keyword or '',
            'message_content': msg.message_content or ''
        } for msg in matched_messages]
        
        df = pd.DataFrame(data)
        
        # 生成 CSV
        output = BytesIO()
        df.to_csv(output, index=False, quoting=csv.QUOTE_MINIMAL, encoding='utf-8-sig')
        output.seek(0)
        
        # 返回文件
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'messages_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        )
    except Exception as e:
        logger.error(f"Error generating CSV: {str(e)}")
        flash(f'导出 CSV 时发生错误: {str(e)}', 'danger')
        return redirect(url_for('messages'))

@app.route('/messages/delete/<int:message_id>')
@login_required
def delete_message(message_id):
    message_to_delete = MatchedMessage.query.get_or_404(message_id)
    db.session.delete(message_to_delete)
    db.session.commit()
    flash('消息已删除。', 'info')
    return redirect(url_for('messages'))

@app.route('/messages/clear_all')
@login_required
def clear_all_messages():
    try:
        num_rows_deleted = db.session.query(MatchedMessage).delete()
        db.session.commit()
        flash(f'已清空 {num_rows_deleted} 条消息。', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'清空消息时出错: {e}', 'danger')
    return redirect(url_for('messages'))

@app.route('/export')
@login_required
def export_page():
    groups = MonitoredGroup.query.order_by(MonitoredGroup.group_name).all()
    tasks = ExportTask.query.order_by(ExportTask.created_at.desc()).all()
    return render_template('export.html', groups=groups, tasks=tasks)

@app.route('/start_export', methods=['POST'])
@login_required
def start_export():
    logger.info("Received request to start export.")
    group_identifier = request.form.get('group_identifier')
    file_format = request.form.get('file_format', 'json')
    logger.info(f"Export parameters: group_identifier={group_identifier}, file_format={file_format}")

    if not group_identifier:
        logger.warning("group_identifier is missing.")
        return jsonify({'success': False, 'error': '未选择群组。'})

    group = MonitoredGroup.query.filter_by(group_identifier=group_identifier).first()
    if not group:
        logger.warning(f"Group with identifier {group_identifier} not found.")
        return jsonify({'success': False, 'error': '所选群组不存在。'})
    
    logger.info(f"Found group: {group.group_name}")

    try:
        new_task = ExportTask(
            group_identifier=group_identifier,
            group_name=group.group_name
        )
        db.session.add(new_task)
        db.session.commit()
        logger.info(f"Successfully created new export task with ID: {new_task.id}")
    except Exception as e:
        logger.error(f"Error creating export task in database: {e}", exc_info=True)
        db.session.rollback()
        return jsonify({'success': False, 'error': f'数据库错误: {e}'})

    thread = Thread(target=run_export_task, args=(new_task.id, group_identifier, file_format, update_export_task_in_db))
    thread.daemon = True
    thread.start()
    logger.info(f"Started background thread for task {new_task.id}")

    return jsonify({'success': True, 'task_id': new_task.id})

@app.route('/task_status/<task_id>')
@login_required
def task_status(task_id):
    task = db.session.get(ExportTask, task_id)
    if not task:
        return jsonify({'error': '任务未找到'}), 404
    return jsonify({
        'id': task.id,
        'status': task.status,
        'file_path': task.file_path,
        'log': task.log
    })

@app.route('/download_export/<task_id>')
@login_required
def download_export(task_id):
    task = ExportTask.query.get_or_404(task_id)
    if task.status == 'completed' and task.file_path and os.path.exists(task.file_path):
        return send_from_directory(os.path.dirname(task.file_path), os.path.basename(task.file_path), as_attachment=True)
    else:
        flash('文件不存在或任务未完成。', 'danger')
        return redirect(url_for('export_page'))

@app.route('/stop_task/<task_id>', methods=['POST'])
@login_required
def stop_task(task_id):
    task = db.session.get(ExportTask, task_id)
    if task and task.status == 'running':
        task.status = 'stopped'
        db.session.commit()
        return jsonify({'success': True, 'message': '停止信号已发送。'})
    return jsonify({'success': False, 'error': '任务未在运行或未找到。'})

@app.route('/delete_task/<task_id>', methods=['POST'])
@login_required
def delete_task(task_id):
    task = db.session.get(ExportTask, task_id)
    if task:
        if task.file_path and os.path.exists(task.file_path):
            try:
                os.remove(task.file_path)
            except OSError as e:
                return jsonify({'success': False, 'error': f'删除文件失败: {e}'})
        db.session.delete(task)
        db.session.commit()
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': '任务未找到'})

if __name__ == '__main__':
    with app.app_context():
        config = Config.query.first()
        if not config:
            print("--- 首次运行配置向导 ---")
            print("未检测到Telegram API配置，请根据提示输入：")
            
            api_id = input("请输入你的 API ID: ").strip()
            api_hash = input("请输入你的 API Hash: ").strip()
            phone_number = input("请输入你的手机号码 (格式如 +8612345678901): ").strip()

            if not (api_id.isdigit() and len(api_hash) > 10 and phone_number.startswith('+')):
                 print("\n错误：输入格式不正确，请检查后重试。\n")
                 exit()

            new_config = Config(
                api_id=api_id,
                api_hash=api_hash,
                phone_number=phone_number
            )
            db.session.add(new_config)
            db.session.commit()
            
            print("\n基础配置已保存！")
            print("请重新启动程序以加载新配置并完成Telegram登录。")
            exit()

    print("检测到配置，正在启动Telegram监控服务，请稍候...")
    start_monitoring()
    
    from telegram_monitor import client_ready
    print("等待客户端完全连接成功...")
    ready = client_ready.wait(timeout=60)
    if not ready:
        print("\n错误：监控服务在60秒内未能成功连接。\n")
        print("请检查您的网络连接、Telegram API凭据是否正确，然后重启程序。")
        exit()
    print("监控服务已就绪！")

    WEB_PORT = 8033

    with app.app_context():
        if User.query.count() == 0:
            default_username = "admin"
            default_password = secrets.token_urlsafe(16)
            hashed_password = generate_password_hash(default_password)
            
            new_admin_user = User(
                username=default_username,
                password_hash=hashed_password,
                is_admin=True
            )
            db.session.add(new_admin_user)
            db.session.commit()
            
            print("------------------------------------------------------")
            print("⚠️ 初次运行：已创建默认管理员用户！")
            print(f"   用户名: {default_username}")
            print(f"   密码:   {default_password}")
            print("   请务必妥善保管此密码。下次登录时将使用此密码。")
            print("------------------------------------------------------")

    print("\n启动Web服务器...")
    print(f"请在浏览器中打开 http://服务器IP:{WEB_PORT}")
    serve(app, host='0.0.0.0', port=WEB_PORT)
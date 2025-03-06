from flask import Blueprint, request, jsonify, render_template, redirect, url_for
from flask_login import current_user, login_required
from sqlalchemy import create_engine, text

from models import EtlLog, User
from tasks import stage_reload_task, dwh_incremental_task
from celery import chain, current_app
from celery.result import AsyncResult
from auth.base_auth import check_auth, authenticate
from celeryconfig import PROD_DB_URI, STAGE_DB_URI, DWH_DB_URI, REDIS_DB_URI
import redis
from .forms import UserProfileForm

admin_blueprint = Blueprint('admin', __name__)

def is_any_task_running():
    inspector = current_app.control.inspect()
    workers = inspector.ping()
    if not workers:
        return False

    for worker_name in workers.keys():
        active_tasks = inspector.active()
        if active_tasks and worker_name in active_tasks and active_tasks[worker_name]:
            return True

    return False

@admin_blueprint.before_request
def require_http_auth():
    auth = request.authorization
    if not auth or not check_auth(auth.username, auth.password):
        return authenticate()

@admin_blueprint.route('/users', methods=['GET'])
@login_required
def users():
    if current_user.is_admin():
        return render_template('admin/users.html', title='DMA - Users', page='users')
    else:
        return redirect(url_for('dashboard.dashboard_index'))

@admin_blueprint.route('/users_data', methods=['GET'])
@login_required
def users_data():
    if not current_user.is_admin():
        return jsonify({"error": "Unauthorized"}), 403

    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('pageSize', 10))
    except ValueError:
        page = 1
        page_size = 10

    sort_field = 'id'
    sort_dir = 'desc'

    sort_field_key = 'sort[0][field]'
    sort_dir_key = 'sort[0][dir]'

    if sort_field_key in request.args and sort_dir_key in request.args:
        sort_field = request.args.get(sort_field_key)
        sort_dir = request.args.get(sort_dir_key)

    query = User.query

    filter_params = []
    i = 0
    while True:
        field = request.args.get(f'filter[{i}][field]')
        type_ = request.args.get(f'filter[{i}][type]')
        value = request.args.get(f'filter[{i}][value]')
        if field and type_ and value:
            filter_params.append({'field': field, 'type': type_, 'value': value})
            i += 1
        else:
            break

    for filter_param in filter_params:
        field = filter_param.get('field')
        type_ = filter_param.get('type')
        value = filter_param.get('value')
        if type_ == 'like':
            query = query.filter(getattr(User, field).like(f"%{value}%"))
        elif type_ == '>':
            query = query.filter(getattr(User, field) > value)
        elif type_ == '<':
            query = query.filter(getattr(User, field) < value)
        elif type_ == '=':
            query = query.filter(getattr(User, field) == value)
        elif type_ == '<=':
            query = query.filter(getattr(User, field) <= value)

    if sort_dir.lower() == 'asc':
        query = query.order_by(getattr(User, sort_field).asc())
    else:
        query = query.order_by(getattr(User, sort_field).desc())

    total_records = query.count()

    app_users = query.offset((page - 1) * page_size).limit(page_size).all()

    data = []
    for user in app_users:
        data.append({
            "id": user.id,
            "email": user.email,
            "role": ("admin" if user.role == 1 else "user"),
            "first_name": user.first_name,
            "last_name": user.last_name,
            "department": user.department,
            "occupation": user.occupation,
            "active": user.active
        })

    return jsonify({
        "last_page": (total_records + page_size - 1) // page_size,
        "data": data
    }), 200

@admin_blueprint.route('/user_edit/<int:user_id>', methods=['GET'])
@login_required
def user_edit(user_id):
    if not current_user.is_admin():
        return redirect(url_for('dashboard.dashboard_index'))

    user = User.query.get(user_id)
    if user:
        form = UserProfileForm()
        form.email.data = user.email
        form.role.data = user.role
        form.first_name.data = user.first_name
        form.last_name.data = user.last_name
        form.department.data = user.department
        form.occupation.data = user.occupation
        form.active.data = user.active
        return render_template('admin/user.html', title='DMA - Edit User', page='users', user=user, form=form)

    return redirect(url_for('admin.users'))

@admin_blueprint.route('/user_edit/<int:user_id>', methods=['POST'])
@login_required
def user_update(user_id):
    if not current_user.is_admin():
        return jsonify({"error": "Unauthorized"}), 403
    form = UserProfileForm()
    if form.validate_on_submit():
        user = User.query.get(user_id)
        if not user:
            return render_template('admin/user.html', form=form, error='User not found', title='DMA - Edit User', page='users')

        find_user = User.query.filter_by(email=form.email.data).first()
        if find_user and find_user.id != user_id:
            return render_template('admin/user.html', form=form, error='Email already exists',
                                   title='DMA - Edit User', page='users')
        if len(form.new_password.data) > 0:
            if len(form.new_password.data) < 6:
                return render_template('admin/user.html', form=form, error='New password must be at least 6 characters', title='DMA - Edit User', page='users')

            user.set_password(form.new_password.data)

        user.email = form.email.data
        user.role = form.role.data
        user.first_name = form.first_name.data
        user.last_name = form.last_name.data
        user.department = form.department.data
        user.occupation = form.occupation.data
        user.active = form.active.data

@admin_blueprint.route('/etl_control', methods=['GET'])
@login_required
def etl_control():
    if not current_user.is_admin():
        return redirect(url_for('dashboard.dashboard_index'))
    return render_template('admin/etl_control.html', title='DMA - ETL Control', page='etl_control')

@admin_blueprint.route('/etl_data', methods=['GET'])
@login_required
def etl_data():
    if not current_user.is_admin():
        return jsonify({"error": "Unauthorized"}), 403

    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('pageSize', 10))
    except ValueError:
        page = 1
        page_size = 10

    sort_field = 'id'
    sort_dir = 'desc'

    sort_field_key = 'sort[0][field]'
    sort_dir_key = 'sort[0][dir]'

    if sort_field_key in request.args and sort_dir_key in request.args:
        sort_field = request.args.get(sort_field_key)
        sort_dir = request.args.get(sort_dir_key)

    query = EtlLog.query

    filter_params = []
    i = 0
    while True:
        field = request.args.get(f'filter[{i}][field]')
        type_ = request.args.get(f'filter[{i}][type]')
        value = request.args.get(f'filter[{i}][value]')
        if field and type_ and value:
            filter_params.append({'field': field, 'type': type_, 'value': value})
            i += 1
        else:
            break

    for filter_param in filter_params:
        field = filter_param.get('field')
        type_ = filter_param.get('type')
        value = filter_param.get('value')
        if type_ == 'like':
            query = query.filter(getattr(EtlLog, field).like(f"%{value}%"))
        elif type_ == '>':
            query = query.filter(getattr(EtlLog, field) > value)
        elif type_ == '<':
            query = query.filter(getattr(EtlLog, field) < value)
        elif type_ == '=':
            query = query.filter(getattr(EtlLog, field) == value)
        elif type_ == '<=':
            query = query.filter(getattr(EtlLog, field) <= value)

    if sort_dir.lower() == 'asc':
        query = query.order_by(getattr(EtlLog, sort_field).asc())
    else:
        query = query.order_by(getattr(EtlLog, sort_field).desc())

    total_records = query.count()

    logs = query.offset((page - 1) * page_size).limit(page_size).all()

    data = []
    for log in logs:
        data.append({
            "id": log.id,
            "job_name": log.job_name,
            "task_id": log.task_id,
            "started_at": log.started_at.isoformat() if log.started_at else None,
            "ended_at": log.ended_at.isoformat() if log.ended_at else None,
            "status": log.status,
            "message": log.message,
            "tables_processed": log.tables_processed,
            "is_active": log.status == "RUNNING"
        })

    return jsonify({
        "last_page": (total_records + page_size - 1) // page_size,
        "data": data
    }), 200

@admin_blueprint.route('/etl_start', methods=['GET'])
@login_required
def run_etl_chain():
    if current_user.is_admin():
        if is_any_task_running():
            return jsonify({"error": "ETL chain is already running."}), 200
        result = chain(stage_reload_task.s(), dwh_incremental_task.s()).apply_async()
        return jsonify({"chain_task_id": result.id, "message": "ETL chain started."}), 200
    else:
        return jsonify({"error": "Unauthorized"}), 403

@admin_blueprint.route('/revoke_task', methods=['POST'])
@login_required
def revoke_task():
    task_id = request.json.get("task_id")
    if not task_id:
        return jsonify({"error": "Task ID is required"}), 200
    task = AsyncResult(task_id)
    if task.state in ['PENDING', 'STARTED']:
        task.revoke(terminate=True)
        return jsonify({"task_id": task_id, "message": "Task revoked."}), 200
    else:
        return jsonify({"error": "Task cannot be revoked in its current state."}), 200

@admin_blueprint.route('/task_status', methods=['GET'])
@login_required
def task_status():
    task_id = request.args.get('task_id')
    if task_id is None:
        return jsonify({"error": "Task ID is required"}), 200
    task = AsyncResult(task_id)
    return jsonify({"task_id": task.id, "task_state": task.state, "task_result": task.result}), 200

@admin_blueprint.route('/prod_db_status', methods=['GET'])
@login_required
def prod_db_status():
    if current_user.is_admin():
        prod_engine = create_engine(PROD_DB_URI)
        with prod_engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            connection.close()
            if result:
                return jsonify({"status": "OK"}), 200
        return jsonify({"error": "Production DB connection error"}), 200
    else:
        return jsonify({"error": "Unauthorized"}), 403

@admin_blueprint.route('/stage_db_status', methods=['GET'])
@login_required
def stage_db_status():
    if current_user.is_admin():
        stage_engine = create_engine(STAGE_DB_URI)
        with stage_engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            connection.close()
            if result:
                return jsonify({"status": "OK"}), 200
        return jsonify({"error": "Stage DB connection error"}), 200
    else:
        return jsonify({"error": "Unauthorized"}), 403

@admin_blueprint.route('/dwh_status', methods=['GET'])
@login_required
def dwh_status():
    if current_user.is_admin():
        dwh_engine = create_engine(DWH_DB_URI)
        with dwh_engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            connection.close()
            if result:
                return jsonify({"status": "OK"}), 200
        return jsonify({"error": "DWH connection error"}), 200
    else:
        return jsonify({"error": "Unauthorized"}), 403

@admin_blueprint.route('/redis_db_status', methods=['GET'])
@login_required
def redis_db_status():
    if current_user.is_admin():
        try:
            redis_client = redis.StrictRedis.from_url(REDIS_DB_URI)
            if redis_client.ping():
                return jsonify({"status": "OK"}), 200
            return jsonify({"error": "Redis connection error"}), 200
        except redis.ConnectionError:
            return jsonify({"error": "Redis connection error"}), 200
    else:
        return jsonify({"error": "Unauthorized"}), 403

@admin_blueprint.route('/celery_worker_status', methods=['GET'])
@login_required
def celery_worker_status():
    if current_user.is_admin():
        inspector = current_app.control.inspect()
        if inspector.ping():
            return jsonify({"status": "OK"}), 200
        else:
            return jsonify({"error": "Celery worker is not running"}), 200
    else:
        return jsonify({"error": "Unauthorized"}), 403
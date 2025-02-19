from flask import Blueprint, request, jsonify, render_template, redirect, url_for
from flask_login import current_user, login_required
from models import EtlLog
from tasks import stage_reload_task, dwh_incremental_task
from celery import chain
from celery.result import AsyncResult

admin_blueprint = Blueprint('admin', __name__)

@admin_blueprint.route('/users', methods=['GET'])
@login_required
def users():
    if current_user.is_admin():
        return jsonify({'message': 'Users'})
    else:
        return redirect(url_for('dashboard.dashboard_index'))

@admin_blueprint.route('/etl_control', methods=['GET'])
@login_required
def etl_control():
    if not current_user.is_admin():
        return redirect(url_for('dashboard.dashboard_index'))
    return render_template('admin/etl_control.html')

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

    sort_field = request.args.get('sortField', 'id')
    sort_dir = request.args.get('sortDir', 'desc')

    query = EtlLog.query

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
            "started_at": log.started_at.isoformat() if log.started_at else None,
            "ended_at": log.ended_at.isoformat() if log.ended_at else None,
            "status": log.status,
            "message": log.message,
            "tables_processed": log.tables_processed,
            "is_active": log.status == "RUNNING"
        })

    return jsonify(data), 200

@admin_blueprint.route('/etl_start', methods=['GET'])
@login_required
def run_etl_chain():
    if current_user.is_admin():
        result = chain(stage_reload_task.s(), dwh_incremental_task.s()).apply_async()
        return jsonify({"chain_task_id": result.id, "message": "ETL chain started."}), 200
    else:
        return jsonify({"error": "Unauthorized"}), 403

@admin_blueprint.route('/revoke_task', methods=['POST'])
@login_required
def revoke_task():
    task_id = request.json.get("task_id")
    if not task_id:
        return jsonify({"error": "Task ID is required"}), 400
    task = AsyncResult(task_id)
    task.revoke(terminate=True)
    return jsonify({"task_id": task_id, "message": "Task revoked."}), 200

@admin_blueprint.route('/task_status', methods=['GET'])
@login_required
def task_status():
    task_id = request.args.get('task_id')
    if task_id is None:
        return jsonify({"error": "Task ID is required"}), 400
    task = AsyncResult(task_id)
    return jsonify({"task_id": task.id, "task_state": task.state, "task_result": task.result}), 200

@admin_blueprint.route('/etl_status', methods=['GET'])
@login_required
def etl_status():
    if current_user.is_admin():
        stage_task = AsyncResult(stage_reload_task().id)
        dwh_task = AsyncResult(dwh_incremental_task().id)
        return jsonify({"stage_task_id": stage_task.id, "stage_task_state": stage_task.state,
                        "dwh_task_id": dwh_task.id, "dwh_task_state": dwh_task.state}), 200
    else:
        return redirect(url_for('dashboard.dashboard_index'))
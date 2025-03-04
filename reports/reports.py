from flask import Blueprint, request, jsonify, render_template, redirect, url_for
from flask_login import current_user, login_required
from sqlalchemy import text, create_engine

from auth.base_auth import check_auth, authenticate
from celeryconfig import PROD_DB_URI, DWH_DB_URI
from models import Report
from reportsconfig import filter_queries, reports_queries
from tasks import build_report_task
from admin.admin import is_any_task_running

reports_blueprint = Blueprint('reports', __name__)

@reports_blueprint.before_request
def require_http_auth():
    auth = request.authorization
    if not auth or not check_auth(auth.username, auth.password):
        return authenticate()

prod_engine = create_engine(PROD_DB_URI)
dwh_engine = create_engine(DWH_DB_URI)

@reports_blueprint.route('/reports', methods=['GET'])
@login_required
def reports_index():
    if not current_user.is_authenticated:
        return redirect(url_for('auth.auth_login_form'))

    with dwh_engine.connect() as conn:
        min_date_row = conn.execute(text(filter_queries["min_date"])).fetchone()
        months = conn.execute(text(filter_queries["months"]), parameters={"min_date": min_date_row.min_date}).fetchall()
        quarters = conn.execute(text(filter_queries["quarters"]), parameters = {"min_date": min_date_row.min_date}).fetchall()
        years = conn.execute(text(filter_queries["years"]), parameters ={"min_date": min_date_row.min_date}).fetchall()

    report_types = {key: value["title"] for key, value in reports_queries.items()}

    return render_template('reports/reports.html', title='DMA - Reports', page='reports', months=months, quarters=quarters, years=years, report_types=report_types)

@reports_blueprint.route('/create_report', methods=['POST'])
@login_required
def create_report():
    if not current_user.is_authenticated:
        return jsonify({"error": "Unauthorized"}), 403

    if is_any_task_running():
        return jsonify({"error": "Another task is running"}), 400

    parameters = request.json.get('parameters')

    if not report_type or not parameters:
        return jsonify({"error": "Missing required parameters"}), 400

    task = build_report_task.delay(current_user.id, parameters)

    return jsonify({"task_id": task.id}), 202

@reports_blueprint.route('/report_data', methods=['GET'])
@login_required
def reports_data():
    if not current_user.is_authenticated:
        return jsonify({"error": "Unauthorized"}), 403

    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('pageSize', 10))
    except ValueError:
        page = 1
        page_size = 10

    sort_field = request.args.get('sortField', 'id')
    sort_dir = request.args.get('sortDir', 'desc')

    query = Report.query

    if sort_dir.lower() == 'asc':
        query = query.order_by(getattr(Report, sort_field).asc())
    else:
        query = query.order_by(getattr(Report, sort_field).desc())

    total_records = query.count()

    reports = query.offset((page - 1) * page_size).limit(page_size).all()

    data = []

    for report in reports:
        data.append({
            "id": report.id,
            "report_type": report.report_type,
            "parameters": report.parameters,
            "result": report.result,
            "started_at": report.started_at,
            "ended_at": report.ended_at,
            "status": report.status,
            "message": report.message
        })

    return jsonify(data), 200
import datetime
import io
import os

from flask import Blueprint, request, jsonify, render_template, redirect, url_for, Response, abort, send_file
from flask_login import current_user, login_required
from sqlalchemy import text, create_engine
import pandas as pd
from auth.base_auth import check_auth, authenticate
from celeryconfig import PROD_DB_URI, DWH_DB_URI
from dashboard.dashboard import apply_period_filter
from models import Report, db
from reportsconfig import filter_queries, reports_queries
from tasks import build_report_task
from playwright.sync_api import sync_playwright

reports_blueprint = Blueprint('reports', __name__)

prod_engine = create_engine(PROD_DB_URI)
dwh_engine = create_engine(DWH_DB_URI)


def apply_period_filter_to_dim(original_query, filter_type, filter_value, range_start, range_end):
    current_date = datetime.datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    current_quarter = (current_month - 1) // 3 + 1
    if filter_type == "month" and filter_value:
        try:
            filter_value_parts = filter_value.split("-")
            filter_value_year = int(filter_value_parts[0])
            filter_value_month = int(filter_value_parts[1])
        except ValueError:
            filter_value_month = current_month
            filter_value_year = current_year
        query = original_query.format(
            filter="p.valid_from <= (date_trunc('month', date '{year}-{month}-01') + interval '1 month - 1 day') AND p.valid_to >= date_trunc('month', date '{year}-{month}-01')".format(year=filter_value_year, month=filter_value_month),
        )
    elif filter_type == "quarter" and filter_value:
        try:
            filter_value_parts = filter_value.split("-")
            filter_value_year = int(filter_value_parts[0])
            filter_value_quarter = int(filter_value_parts[1])
        except ValueError:
            filter_value_quarter = current_quarter
            filter_value_year = current_year
        query = original_query.format(
            filter="p.valid_from <= (make_date({year}, ({quarter} - 1) * 3 + 1, 1) + interval '3 months - 1 day') AND p.valid_to >= make_date({year}, ({quarter} - 1) * 3 + 1, 1)".format(year=filter_value_year, quarter=filter_value_quarter))
    elif filter_type == "year" and filter_value:
        try:
            filter_value = int(filter_value)
        except ValueError:
            filter_value = 0
        query = original_query.format(
            filter="p.valid_from <= make_date({year}, 12, 31) AND p.valid_to >= make_date({year}, 1, 1)".format(year=filter_value),
        )
    elif filter_type == "range" and range_start or range_end:
        try:
            date_start = range_start
            filter_value_start = datetime.datetime.strptime(date_start, "%Y-%m-%d").date()
            date_end = range_end
            filter_value_end = datetime.datetime.strptime(date_end, "%Y-%m-%d").date()
        except ValueError:
            filter_value_start = filter_value_end = datetime.datetime.strptime(current_date, "%Y-%m-%d").date()
        query = original_query.format(
            filter="p.valid_from <= {end_date} AND p.valid_to >= {start_date}".format(start_date=filter_value_start, end_date=filter_value_end),
        )
    else:
        current_year = datetime.datetime.now().year
        query = original_query.format(
            filter="p.valid_from <= make_date({year}, 12, 31) AND p.valid_to >= make_date({year}, 1, 1)".format(year=current_year),
        )
    return query

@reports_blueprint.before_request
def require_http_auth():
    auth = request.authorization
    if not auth or not check_auth(auth.username, auth.password):
        return authenticate()

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

        report_types = {}
        for key, value in reports_queries.items():
            report_types[key] = {"title": value["title"]}
            if "subfilters" in value:
                report_types[key]["subfilters"] = {}
                for subfilter_key, subfilter_value in value["subfilters"].items():
                    report_types[key]["subfilters"][subfilter_key] = {"title": subfilter_value["title"]}

    return render_template('reports/reports.html', title='DMA - Správy', page='reports', months=months, quarters=quarters, years=years, report_types=report_types)

@reports_blueprint.route('/get_subfilter_options', methods=['POST'])
@login_required
def get_subfilter_options():
    if not current_user.is_authenticated:
        return jsonify({"error": "Neautorizovaný prístup"}), 403

    current_date = datetime.datetime.now()
    report_type = request.json.get('report_type')
    subfilter = request.json.get('subfilter')

    if not report_type or not subfilter:
        return jsonify({"error": "Chýbajúce povinné parametre"}), 200

    if report_type not in reports_queries:
        return jsonify({"error": "Neplatný typ správy"}), 200

    if subfilter and subfilter not in reports_queries[report_type]["subfilters"]:
        return jsonify({"error": "Neplatný podfilter"}), 200

    menu_query = reports_queries[report_type]["subfilters"][subfilter].get("menu_query")
    date_filter_type = request.json.get("date_filter_type")
    date_filter_value = request.json.get("date_filter_value")
    date_filter_type = "year" if date_filter_type is None else date_filter_type
    date_filter_value = str(current_date.year) if date_filter_value is None else date_filter_value
    range_start = range_end = None
    if (date_filter_type == "range"):
        range_start = request.args.get("filter_value_start")
        range_end = request.args.get("filter_value_end")
    query = apply_period_filter_to_dim(menu_query, date_filter_type, date_filter_value, range_start, range_end)

    if not menu_query:
        return jsonify({"error": "Chýba dotaz pre menu"}), 200

    with dwh_engine.connect() as conn:
        elements = conn.execute(text(query)).fetchall()
        elements.insert(0, ('[Any]',))

    return jsonify({"elements": [{"key": row[0], "value": row[0]} for row in elements]}), 200

@reports_blueprint.route('/create_report', methods=['POST'])
@login_required
def create_report():
    if not current_user.is_authenticated:
        return jsonify({"error": "Neautorizovaný prístup"}), 403

    report_type = request.json.get('report_type')
    subfilters = request.json.get('subfilters')

    if not report_type:
        return jsonify({"error": "Chýbajúce povinné parametre"}), 200

    if report_type not in reports_queries:
        return jsonify({"error": "Neplatný typ správy"}), 200

    where_clause = []
    report_subfilters = []

    for subfilter_key, subfilter_value  in subfilters.items():
        if subfilter_key in reports_queries[report_type]["subfilters"]:
            if subfilter_value != "[Any]":
                if subfilter_key == "market_group":
                    where_clause.append(f"dp.market_group = '{subfilter_value}'")
                elif subfilter_key == "market_subgroup":
                    where_clause.append(f"dp.market_subgroup = '{subfilter_value}'")
                elif subfilter_key == "market_gender":
                    where_clause.append(f"dp.market_gender = '{subfilter_value}'")
            report_subfilters.append({subfilter_key: {'title': reports_queries[report_type]["subfilters"][subfilter_key]['title'],'value': subfilter_value}})

    if len(where_clause) == 0:
        where_clause.append("1 = 1")

    date_filter_type = request.json.get("date_filter_type")
    date_filter_value = request.json.get("date_filter_value")

    current_date = datetime.datetime.now()

    date_filter_type = "year" if date_filter_type is None else date_filter_type
    date_filter_value = str(current_date.year) if date_filter_value is None else date_filter_value
    range_start = range_end = None
    if (date_filter_type == "range"):
        range_start = request.json.get("start_date_filter")
        range_end = request.json.get("end_date_filter")

    query = reports_queries[report_type].get("query")

    if not query:
        return jsonify({"error": "No query"}), 200

    query = apply_period_filter(query, current_date, date_filter_type, date_filter_value, range_start, range_end)
    query = query.format(group_filter=" AND ".join(where_clause))

    prep_query = reports_queries[report_type].get("prep_query")
    if prep_query:
        for i in range(len(prep_query)):
            prep_query[i] = apply_period_filter(prep_query[i], current_date, date_filter_type, date_filter_value, range_start, range_end)
            prep_query[i] = prep_query[i].format(group_filter=" AND ".join(where_clause))

    parameters = {
        "user_id": current_user.id,
        "report_type": report_type,
        "report_title": reports_queries[report_type]["title"],
        "report_data_type": reports_queries[report_type]["data_type"] if "data_type" in reports_queries[report_type] else "diagram",
        "report_diagram_type": reports_queries[report_type]["diagram_type"] if "diagram_type" in reports_queries[report_type] else "bar",
        "show_diagram_table": reports_queries[report_type]["show_diagram_table"] if "show_diagram_table" in reports_queries[report_type] else True,
        "prep_query": prep_query,
        "query": query,
        "filters": {
            "date_filter_type": date_filter_type,
            "date_filter_value": date_filter_value,
            "range_start": range_start,
            "range_end": range_end,
            "report_subfilters": report_subfilters
        }
    }

    task = build_report_task.delay(parameters)

    return jsonify({"task_id": task.id}), 200

@reports_blueprint.route('/report_data', methods=['GET'])
@login_required
def reports_data():
    if not current_user.is_authenticated:
        return jsonify({"error": "Neautorizovaný prístup"}), 403

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

    query = Report.query

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

    if not current_user.is_admin():
        query = query.filter(Report.user_id == current_user.id)

    for filter_param in filter_params:
        field = filter_param.get('field')
        type_ = filter_param.get('type')
        value = filter_param.get('value')
        if type_ == 'like':
            query = query.filter(getattr(Report, field).like(f"%{value}%"))
        elif type_ == '>':
            query = query.filter(getattr(Report, field) > value)
        elif type_ == '<':
            query = query.filter(getattr(Report, field) < value)
        elif type_ == '=':
            query = query.filter(getattr(Report, field) == value)
        elif type_ == '<=':
            query = query.filter(getattr(Report, field) <= value)

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
            "started_at": report.started_at.isoformat() if report.started_at else None,
            "ended_at": report.ended_at.isoformat() if report.ended_at else None,
            "status": report.status,
            "message": report.message
        })

    return jsonify({
        "last_page": (total_records + page_size - 1) // page_size,
        "data": data
    }), 200

@reports_blueprint.route('/view_report/<int:report_id>', methods=['GET'])
@login_required
def view_report(report_id):
    report = db.session.get(Report, report_id)

    if report:
        if report.user_id != current_user.id and not current_user.is_admin():
            return redirect(url_for('dashboard.dashboard_index'))

        created_date = report.ended_at.strftime("%Y-%m-%d %H:%M:%S") if report.ended_at else None

        report_data_type = report.parameters.get("report_data_type") if report.parameters.get("report_data_type") else "diagram"

        if report_data_type == "table":
            return render_template('reports/report_table.html', title='DMA - Správa', page='view_report', report=report, report_parameters=report.parameters, report_data_type=report_data_type,)
        elif report_data_type == "diagram":
            return render_template('reports/report.html', title='DMA - Správa', page='view_report', report=report, report_parameters=report.parameters)

    return redirect(url_for('reports.reports_index'))

@reports_blueprint.route('/generate-pdf', methods=['POST'])
@login_required
def generate_pdf():
    if not current_user.is_authenticated:
        return jsonify({"error": "Neautorizovaný prístup"}), 403

    report_id = request.json.get('report_id')

    if not report_id:
        return jsonify({"error": "Chýbajúce povinné parametre"}), 200

    report = db.session.get(Report, report_id)

    if report:
        if report.user_id != current_user.id and not current_user.is_admin():
            return redirect(url_for('dashboard.dashboard_index'))

        html_content = render_template('reports/report_pdf.html', title='DMA - Správa', page='pdf_report', report=report, report_parameters=report.parameters)

        pdf_buffer = io.BytesIO()
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
            page = browser.new_page()
            page.set_viewport_size({"width": 768, "height": 1024})

            page.set_content(html_content, wait_until='domcontentloaded')

            page.emulate_media(media="print")
            pdf_data = page.pdf(
                format='A4',
                print_background=True,
                margin={'top': '20mm', 'right': '20mm', 'bottom': '20mm', 'left': '20mm'},
                scale=0.8
            )
            pdf_buffer.write(pdf_data)
            pdf_buffer.seek(0)
            browser.close()

        return Response(
            pdf_buffer.getvalue(),
            mimetype='application/pdf',
            headers={'Content-Disposition': 'attachment; filename=report.pdf'}
        )

    return jsonify({"error": "Správa neexistuje"}), 200


@reports_blueprint.route('/get-csv-table/<int:report_id>')
@login_required
def get_csv_table(report_id):
    if not current_user.is_authenticated:
        return jsonify({"error": "Neautorizovaný prístup"}), 403

    # report = Report.query.get(report_id)
    report = db.session.get(Report, report_id)

    if report:
        if report.user_id != current_user.id and not current_user.is_admin():
            return redirect(url_for('dashboard.dashboard_index'))

        try:
            page = int(request.args.get('page', 1))
            page_size = int(request.args.get('pageSize', 10))
        except ValueError:
            page = 1
            page_size = 10

        filepath = report.result.get('filepath')
        total_records = report.result.get('total_rows')

        columns = report.result.get('columns')

        if not os.path.exists(filepath):
            return jsonify({'error': 'Súbor neexistuje'}), 200

        df = pd.read_csv(filepath, header=None, names=columns, skiprows=(page - 1) * page_size + 1, nrows=page_size)
        rows = df.to_dict(orient='records')

        data = []

        for row in rows:
            data.append(row)

        return jsonify({
            "last_page": (total_records - 1 + page_size - 1) // page_size,
            "data": data
        }), 200

    return jsonify({"error": "Správa neexistuje"}), 200

@reports_blueprint.route('/download-csv-report/<int:report_id>')
@login_required
def download_csv_report(report_id):
    if not current_user.is_authenticated:
        abort(403)

    report = db.session.get(Report, report_id)

    if report:
        if report.user_id != current_user.id and not current_user.is_admin():
            return redirect(url_for('dashboard.dashboard_index'))

    filepath = report.result.get('filepath')

    if not os.path.exists(filepath):
        abort(404)

    return send_file(filepath, as_attachment=True, download_name='report.csv', mimetype='text/csv')
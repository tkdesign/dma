from flask import Blueprint, request, jsonify, render_template, redirect, url_for
from flask_login import current_user, login_required

from auth.base_auth import check_auth, authenticate

reports_blueprint = Blueprint('reports', __name__)

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
    return render_template('reports/reports.html', title='DMA - Reports')
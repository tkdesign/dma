from flask import Blueprint, request, jsonify, render_template, redirect, url_for
from flask_login import current_user, login_required

reports_blueprint = Blueprint('reports', __name__)

@reports_blueprint.route('/reports', methods=['GET'])
@login_required
def reports_index():
    if not current_user.is_authenticated:
        return redirect(url_for('auth.auth_login_form'))
    return render_template('reports/reports.html', title='DMA - Reports')
from flask import Blueprint, request, jsonify, render_template, redirect, url_for
from flask_login import current_user, login_required

dashboard_blueprint = Blueprint('dashboard', __name__)

@dashboard_blueprint.route('/dashboard', methods=['GET'])
@login_required
def dashboard_index():
    if not current_user.is_authenticated:
        return redirect(url_for('auth.auth_login_form'))
    return render_template('dashboard/dashboard.html', title='DMA - Dashboard')
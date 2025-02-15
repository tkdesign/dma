from flask import Blueprint, request, jsonify, render_template, redirect, url_for
from flask_login import current_user, login_required

admin_blueprint = Blueprint('admin', __name__)

@admin_blueprint.route('/admin', methods=['GET'])
@login_required
def admin_index():
    if current_user.is_admin():
        return jsonify({'message': 'Admin index'})
    else:
        return redirect(url_for('dashboard.dashboard_index'))
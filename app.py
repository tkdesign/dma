from flask import Flask, Blueprint, render_template, redirect, url_for
from flask_login import LoginManager, current_user, login_required
from auth.auth import auth_blueprint
from dashboard.dashboard import dashboard_blueprint
from reports.reports import reports_blueprint
from admin.admin import admin_blueprint
from models import db, User

app = Flask(__name__)
app.config.from_pyfile('config.py')
app.secret_key = 'ukf_8#4!2'

db.init_app(app)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'auth.auth_login_form'
@login_manager.user_loader
def load_user(user_id):
    return User.get(user_id)

home_bp = Blueprint('home', __name__, url_prefix='/')

@home_bp.route('/')
def home():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.dashboard_index'))
    else:
        return render_template('index.html', title='DMA - e-commerce data management application')

app.register_blueprint(auth_blueprint)
app.register_blueprint(dashboard_blueprint)
app.register_blueprint(reports_blueprint)
app.register_blueprint(home_bp)
app.register_blueprint(admin_blueprint)

if __name__ == '__main__':
    app.run()

from flask import Flask, Blueprint, render_template, redirect, url_for, request
from flask_login import LoginManager, current_user
from auth.auth import auth_blueprint
from auth.base_auth import check_auth, authenticate
from dashboard.dashboard import dashboard_blueprint
from reports.reports import reports_blueprint
from admin.admin import admin_blueprint
from models import db, User

def create_app(config_override=None):
    app = Flask(__name__)
    app.config.from_pyfile('config.py')

    if config_override:
        app.config.update(config_override)

    db.init_app(app)

    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'auth.auth_login_form'

    @login_manager.user_loader
    def load_user(user_id):
        return User.get(user_id)

    home_bp = Blueprint('home', __name__, url_prefix='/')

    @home_bp.before_request
    def require_http_auth():
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()

    @home_bp.route('/')
    def home():
        if current_user.is_authenticated:
            return redirect(url_for('dashboard.dashboard_index'))
        else:
            return render_template('index.html', title='DMA - Aplikácia pre správu ecommerce dát')

    @app.errorhandler(404)
    def page_not_found(e):
        if current_user.is_authenticated:
            return render_template('error.html', layout='user/layout.html', error_code=404, error_message='Ups! Stránka, ktorú hľadáte, neexistuje.', redirect_url='dashboard.dashboard_index', redirect_text='Prejsť na informačný panel'), 404
        return render_template('error.html', layout='guest/layout.html', error_code=404, error_message='Ups! Stránka, ktorú hľadáte, neexistuje.', redirect_url='home.home', redirect_text='Prejsť na hlavnú stránku'), 404

    @app.errorhandler(403)
    def forbidden(e):
        return render_template('error.html', layout='guest/layout.html', error_code=403, error_message='Nemáte povolenie na prístup k tejto stránke.', redirect_url='home.home', redirect_text='Prejsť na hlavnú stránku'), 403
    @app.errorhandler(500)
    def internal_server_error(e):
        if current_user.is_authenticated:
            return render_template('error.html', layout='user/layout.html', error_code=500, error_message='Vyskytla sa interná chyba servera.', redirect_url='dashboard.dashboard_index', redirect_text='Prejsť na informačný panel'), 500
        return render_template('error.html', layout='guest/layout.html', error_code=500, error_message='Vyskytla sa interná chyba servera.', redirect_url='home.home', redirect_text='Prejsť na hlavnú stránku'), 500

    app.register_blueprint(auth_blueprint)
    app.register_blueprint(dashboard_blueprint)
    app.register_blueprint(reports_blueprint)
    app.register_blueprint(home_bp)
    app.register_blueprint(admin_blueprint)

    return app

if __name__ == '__main__':
    app = create_app()
    app.run()

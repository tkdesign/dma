from flask import Blueprint, render_template, redirect, url_for, request
from flask_login import current_user, login_required, login_user, logout_user

from models import User
from .forms import LoginForm, RegistrationForm, ProfileForm

from auth.base_auth import check_auth, authenticate

auth_blueprint = Blueprint('auth', __name__)

@auth_blueprint.before_request
def require_http_auth():
    auth = request.authorization
    if not auth or not check_auth(auth.username, auth.password):
        return authenticate()

@auth_blueprint.route('/signup', methods=['GET'])
def auth_signup_form():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.dashboard_index'))
    form = RegistrationForm()
    return render_template('auth/signup.html', form=form, title='DMA - Registrácia', page='signup')

@auth_blueprint.route('/signup', methods=['POST'])
def auth_signup():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.dashboard_index'))
    form = RegistrationForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        if user:
            return render_template('auth/signup.html', form=form, error='Email already exists', title='DMA - Registrácia')
        user = User(email=form.email.data, password='', role=2, first_name=form.first_name.data, last_name=form.last_name.data, occupation=form.occupation.data, department=form.department.data, active=False)
        user.set_password(form.password.data)
        user.save()
        return redirect(url_for('auth.auth_login_form'))
    return render_template('auth/signup.html', form=form, title='DMA - Registrácia', page='signup')

@auth_blueprint.route('/login', methods=['GET'])
def auth_login_form():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.dashboard_index'))
    form = LoginForm()
    return render_template('auth/login.html', form=form, title='DMA - Prihlásenie', page='login')

@auth_blueprint.route('/login', methods=['POST'])
def auth_login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.dashboard_index'))
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        if not user or not user.check_password(form.password.data) or not user.is_active():
            return render_template('auth/login.html', form=form, error='Neplatný e-mail alebo heslo', title='DMA - Prihlásenie')
        login_user(user, remember=form.remember.data)
        return redirect(url_for('dashboard.dashboard_index'))
    return render_template('auth/login.html', form=form, title='DMA - Prihlásenie', page='login')

@auth_blueprint.route('/logout', methods=['GET'])
@login_required
def auth_logout():
    if current_user.is_authenticated:
        logout_user()
    return redirect(url_for('auth.auth_login_form'))

@auth_blueprint.route('/profile', methods=['GET'])
@login_required
def auth_profile():
    form = ProfileForm()
    form.email.data = current_user.email
    form.current_password.data = ''
    form.new_password.data = ''
    form.first_name.data = current_user.first_name
    form.last_name.data = current_user.last_name
    form.occupation.data = current_user.occupation
    form.department.data = current_user.department
    return render_template('auth/profile.html', title='DMA - Profil', form=form, page='profile')

@auth_blueprint.route('/profile', methods=['POST'])
@login_required
def auth_profile_update():
    form = ProfileForm()
    if form.validate_on_submit():
        if len(form.current_password.data) > 0 or len(form.new_password.data) > 0:
            if form.current_password.data == form.new_password.data:
                return render_template('auth/profile.html', form=form, error='Súčasné heslo a nové heslo nemôžu byť rovnaké', title='DMA - Profil', page='profile')
            if not current_user.check_password(form.current_password.data):
                return render_template('auth/profile.html', form=form, error='Neplatné súčasné heslo', title='DMA - Profil', page='profile')
            if current_user.check_password(form.new_password.data):
                return render_template('auth/profile.html', form=form, error='Nové heslo nemôže byť rovnaké ako súčasné heslo', title='DMA - Profil', page='profile')
            if len(form.new_password.data) < 6:
                return render_template('auth/profile.html', form=form, error='Nové heslo musí mať aspoň 6 znakov', title='DMA - Profil', page='profile')
            current_user.set_password(form.new_password.data)
        current_user.first_name = form.first_name.data
        current_user.last_name = form.last_name.data
        current_user.occupation = form.occupation.data
        current_user.department = form.department.data
        current_user.save()
        return redirect(url_for('auth.auth_profile'))
    return render_template('auth/profile.html', form=form, title='DMA - Profil', page='profile')

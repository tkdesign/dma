from flask import Blueprint, render_template, redirect, url_for
from flask_login import current_user, login_required, login_user, logout_user

from models import User
from .forms import LoginForm, RegistrationForm

auth_blueprint = Blueprint('auth', __name__)

@auth_blueprint.route('/signup', methods=['GET'])
def auth_signup_form():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.dashboard_index'))
    form = RegistrationForm()
    return render_template('auth/signup.html', form=form, title='DMA - Registration')

@auth_blueprint.route('/signup', methods=['POST'])
def auth_signup():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.dashboard_index'))
    form = RegistrationForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        if user:
            return render_template('auth/signup.html', form=form, error='Email already exists', title='DMA - Registration')
        user = User(email=form.email.data, password='', role=2, first_name=form.first_name.data, last_name=form.last_name.data, occupation=form.occupation.data, department=form.department.data, active=False)
        user.set_password(form.password.data)
        user.save()
        return redirect(url_for('auth.auth_login_form'))
    return render_template('auth/signup.html', form=form, title='DMA - Registration')

@auth_blueprint.route('/login', methods=['GET'])
def auth_login_form():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.dashboard_index'))
    form = LoginForm()
    return render_template('auth/login.html', form=form, title='DMA - Login')

@auth_blueprint.route('/login', methods=['POST'])
def auth_login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard.dashboard_index'))
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        if not user or not user.check_password(form.password.data) or not user.is_active():
            return render_template('auth/login.html', form=form, error='Invalid email or password', title='DMA - Login')
        login_user(user, remember=form.remember.data)
        return redirect(url_for('dashboard.dashboard_index'))
    return render_template('auth/login.html', form=form, title='DMA - Login')

@auth_blueprint.route('/logout', methods=['GET'])
@login_required
def auth_logout():
    if current_user.is_authenticated:
        logout_user()
    return redirect(url_for('auth.auth_login_form'))

@auth_blueprint.route('/profile', methods=['GET'])
@login_required
def auth_profile():
    form = RegistrationForm()
    form.email.data = current_user.email
    form.first_name.data = current_user.first_name
    form.last_name.data = current_user.last_name
    form.occupation.data = current_user.occupation
    form.department.data = current_user.department
    return render_template('auth/profile.html', title='DMA - Profile', form=form)

@auth_blueprint.route('/settings', methods=['GET'])
@login_required
def auth_settings():
    return render_template('auth/settings.html', title='DMA - Settings')

@auth_blueprint.route('/settings', methods=['POST'])
@login_required
def auth_settings_update():
    return redirect(url_for('auth.auth_settings'))
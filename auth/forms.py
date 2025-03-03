from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField
from wtforms.validators import DataRequired, Email, EqualTo, Length

class RegistrationForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()], default='')
    password = PasswordField('Password', validators=[DataRequired(), Length(min=6)], default='')
    confirm_password = PasswordField('Confirm Password', validators=[DataRequired(), EqualTo('password')], default='')
    first_name = StringField('First Name', validators=[DataRequired()], default='')
    last_name = StringField('Last Name', validators=[DataRequired()], default='')
    department = StringField('Department', validators=[DataRequired()], default='')
    occupation = StringField('Occupation', validators=[DataRequired()], default='')
    submit = SubmitField('Sign Up')

class ProfileForm(FlaskForm):
    email = StringField('Email', validators=[], default='')
    current_password = PasswordField('Current password', validators=[], default='')
    new_password = PasswordField('New password', validators=[], default='')
    first_name = StringField('First Name', validators=[DataRequired()], default='')
    last_name = StringField('Last Name', validators=[DataRequired()], default='')
    department = StringField('Department', validators=[DataRequired()], default='')
    occupation = StringField('Occupation', validators=[DataRequired()], default='')
    submit = SubmitField('Update')

class LoginForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()], default='')
    password = PasswordField('Password', validators=[DataRequired()], default='')
    remember = BooleanField('Remember Me')
    submit = SubmitField('Login')
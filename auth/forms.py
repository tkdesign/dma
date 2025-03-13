from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField
from wtforms.validators import DataRequired, Email, EqualTo, Length

class RegistrationForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()], default='')
    password = PasswordField('Heslo', validators=[DataRequired(), Length(min=6)], default='')
    confirm_password = PasswordField('Potvrdenie hesla', validators=[DataRequired(), EqualTo('password')], default='')
    first_name = StringField('Meno', validators=[DataRequired()], default='')
    last_name = StringField('Priezvisko', validators=[DataRequired()], default='')
    department = StringField('Oddelenie', validators=[DataRequired()], default='')
    occupation = StringField('Pozícia', validators=[DataRequired()], default='')
    submit = SubmitField('Zaregistrovať sa')

class ProfileForm(FlaskForm):
    email = StringField('Email', validators=[], default='')
    current_password = PasswordField('Súčasné heslo', validators=[], default='')
    new_password = PasswordField('Nové heslo', validators=[], default='')
    first_name = StringField('Meno', validators=[DataRequired()], default='')
    last_name = StringField('Priezvisko', validators=[DataRequired()], default='')
    department = StringField('Oddelenie', validators=[DataRequired()], default='')
    occupation = StringField('Pozícia', validators=[DataRequired()], default='')
    submit = SubmitField('Aktualizovať')

class LoginForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()], default='')
    password = PasswordField('Password', validators=[DataRequired()], default='')
    remember = BooleanField('Zapamätať si ma')
    submit = SubmitField('Prihlásiť sa')
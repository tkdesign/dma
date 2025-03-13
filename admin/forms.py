from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField
from wtforms.fields.choices import SelectField
from wtforms.validators import DataRequired, Email, EqualTo, Length

class UserProfileForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()], default='')
    new_password = PasswordField('Nové heslo', validators=[], default='')
    role = SelectField('Rola', choices=[(1, 'Správca'), (2, 'Používateľ')], coerce=int)
    first_name = StringField('Meno', validators=[DataRequired()], default='')
    last_name = StringField('Priezvisko', validators=[DataRequired()], default='')
    department = StringField('Oddelenie', validators=[DataRequired()], default='')
    occupation = StringField('Pozícia', validators=[DataRequired()], default='')
    active = BooleanField('Aktívny')
    submit = SubmitField('Aktualizácia')

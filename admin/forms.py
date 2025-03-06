from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField
from wtforms.fields.choices import SelectField
from wtforms.validators import DataRequired, Email, EqualTo, Length

class UserProfileForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()], default='')
    new_password = PasswordField('New password', validators=[], default='')
    role = SelectField('Role', choices=[(1, 'Admin'), (2, 'User')], coerce=int)
    first_name = StringField('First Name', validators=[DataRequired()], default='')
    last_name = StringField('Last Name', validators=[DataRequired()], default='')
    department = StringField('Department', validators=[DataRequired()], default='')
    occupation = StringField('Occupation', validators=[DataRequired()], default='')
    active = BooleanField('Active')
    submit = SubmitField('Update')

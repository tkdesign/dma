from werkzeug.security import generate_password_hash, check_password_hash
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
db = SQLAlchemy()

class User(UserMixin, db.Model):
    __bind_key__ = 'dwh'
    __tablename__ = 'user'
    __table_args__ = {'schema': 'public'}
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)
    role = db.Column(db.Integer, default=2)
    first_name = db.Column(db.String(255), nullable=False)
    last_name = db.Column(db.String(255), nullable=False)
    department = db.Column(db.String(255), nullable=True)
    occupation = db.Column(db.String(255), nullable=True)
    active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), server_onupdate=db.func.now())

    def __repr__(self):
        return '<User {}>'.format(self.username)

    def __init__(self, email, password, role, first_name, last_name, department, occupation, active):
        self.email = email
        self.password = generate_password_hash(password)
        self.role = role
        self.first_name = first_name
        self.last_name = last_name
        self.department = department
        self.occupation = occupation
        self.active = active

    def set_password(self, password):
        self.password = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password, password)

    def is_admin(self):
        return self.role == 1

    def is_active(self):
        return self.active

    def save(self):
        db.session.add(self)
        db.session.commit()

    def get(user_id):
        return User.query.get(user_id)

class Dashboard(db.Model):
    __bind_key__ = 'dwh'
    __tablename__ = 'dashboard'
    __table_args__ = {'schema': 'public'}
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('public.user.id'), nullable=False)
    name = db.Column(db.String(255), nullable=False)
    description = db.Column(db.String(255), nullable=True)
    active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), server_onupdate=db.func.now())

    def __repr__(self):
        return '<Dashboard {}>'.format(self.name)

    def save(self):
        db.session.add(self)
        db.session.commit()

    def get(dashboard_id):
        return Dashboard.query.get(dashboard_id)

    def get_by_user(user_id):
        return Dashboard.query.filter_by(user_id=user_id).all()
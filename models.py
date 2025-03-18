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
        return '<Používateľ {}>'.format(self.username)

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

class EtlLog(db.Model):
    __bind_key__ = 'stage'
    __tablename__ = 'etl_log'
    __table_args__ = {'schema': 'dma_db_stage'}
    id = db.Column(db.Integer, primary_key=True)
    job_name = db.Column(db.String(255), nullable=False)
    started_at = db.Column(db.DateTime, nullable=False)
    ended_at = db.Column(db.DateTime, nullable=True)
    status = db.Column(db.String(50), nullable=False)
    message = db.Column(db.Text, nullable=True)
    tables_processed = db.Column(db.Integer, nullable=True)
    task_id = db.Column(db.String(36), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return '<EtlLog {}>'.format(self.job_name)

    def __init__(self, job_name, started_at, status):
        self.job_name = job_name
        self.started_at = started_at
        self.status = status

    def save(self):
        db.session.add(self)
        db.session.commit()

    def get(log_id):
        return EtlLog.query.get(log_id)

class Report(db.Model):
    __bind_key__ = 'dwh'
    __tablename__ = 'report'
    __table_args__ = {'schema': 'public'}
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    report_type = db.Column(db.String(50), nullable=False)
    parameters = db.Column(db.JSON, nullable=False)
    result = db.Column(db.JSON, nullable=True)
    started_at = db.Column(db.DateTime, nullable=False)
    ended_at = db.Column(db.DateTime, nullable=True)
    status = db.Column(db.String(50), nullable=False)
    message = db.Column(db.Text, nullable=True)
    task_id = db.Column(db.String(36), nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def __repr__(self):
        return '<Správa {}>'.format(self.id)

    def __init__(self, user_id, report_type, parameters, started_at, status, task_id):
        self.user_id = user_id
        self.report_type = report_type
        self.parameters = parameters
        self.started_at = started_at
        self.status = status
        self.task_id = task_id

    def save(self):
        db.session.add(self)
        db.session.commit()

    def get(report_id):
        return Report.query.get(report_id)
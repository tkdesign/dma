from sqlalchemy import create_engine
from load_dim_date import load_dim_date
from celeryconfig import DWH_DB_URI

dwh_engine = create_engine(DWH_DB_URI)

load_dim_date(self=None, stage_engine=None, dwh_engine=dwh_engine)
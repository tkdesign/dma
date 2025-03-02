from sqlalchemy import create_engine
from load_dim_time import load_dim_time
from celeryconfig import DWH_DB_URI

dwh_engine = create_engine(DWH_DB_URI)

load_dim_time(self=None, stage_engine=None, dwh_engine=dwh_engine)
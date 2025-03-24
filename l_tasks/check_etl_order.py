from sqlalchemy import create_engine
from load_to_dwh import load_fact_order
from celeryconfig import STAGE_DB_URI, DWH_DB_URI

dwh_engine = create_engine(DWH_DB_URI)

load_fact_order(self=None, stage_engine=None, dwh_engine=dwh_engine)
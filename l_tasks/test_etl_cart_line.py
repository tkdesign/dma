from sqlalchemy import create_engine
from load_to_dwh import load_fact_cart_line
from celeryconfig import STAGE_DB_URI, DWH_DB_URI

stage_engine = create_engine(STAGE_DB_URI)
dwh_engine = create_engine(DWH_DB_URI)

load_fact_cart_line(self=None, stage_engine=stage_engine, dwh_engine=dwh_engine)
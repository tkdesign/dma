from sqlalchemy import create_engine
from load_dim_attribute import load_dim_attribute
from celeryconfig import STAGE_DB_URI, DWH_DB_URI

stage_engine = create_engine(STAGE_DB_URI)
dwh_engine = create_engine(DWH_DB_URI)

load_dim_attribute(self=None, stage_engine=stage_engine, dwh_engine=dwh_engine)
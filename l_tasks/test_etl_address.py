from sqlalchemy import create_engine
from load_dim_address import load_dim_address
from celeryconfig import STAGE_DB_URI, DWH_DB_URI

stage_engine = create_engine(STAGE_DB_URI)
dwh_engine = create_engine(DWH_DB_URI)

load_dim_address(self=None, stage_engine=stage_engine, dwh_engine=dwh_engine)
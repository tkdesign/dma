SQLALCHEMY_TRACK_MODIFICATIONS = False
SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://username:password@127.0.0.1/dma_dwh'
SQLALCHEMY_BINDS = {
    'dwh': 'postgresql+psycopg2://username:password@127.0.0.1/dma_dwh',
    'prod': 'mysql+pymysql://username:password@127.0.0.1/dbname',
    'stage': 'postgresql+psycopg2://username:password@127.0.0.1/dma_stage'
}
USERS = {
    'employee': 'password_hash_string'  # md5('password')
}
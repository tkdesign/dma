from sqlalchemy import text

def load_dim_date(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Task aborted.")
        return

    dwh_query = """
    BEGIN;
    SET CONSTRAINTS ALL DEFERRED;
    CREATE OR REPLACE FUNCTION generate_dim_date(start_date date, end_date date)
    RETURNS void AS $$
    BEGIN
        TRUNCATE TABLE public.dim_date CASCADE;    
        INSERT INTO public.dim_date (
            date, 
            year, 
            quarter, 
            month, 
            month_name, 
            day, 
            day_of_week, 
            day_name, 
            week_of_year, 
            is_weekend
        )
        SELECT 
            gs::date,
            EXTRACT(YEAR FROM gs)::integer,
            EXTRACT(QUARTER FROM gs)::integer,
            EXTRACT(MONTH FROM gs)::integer,
            TO_CHAR(gs, 'FMMonth'),
            EXTRACT(DAY FROM gs)::integer,
            EXTRACT(ISODOW FROM gs)::integer,
            TO_CHAR(gs, 'FMDay'),
            EXTRACT(WEEK FROM gs)::integer,
            CASE 
                WHEN EXTRACT(ISODOW FROM gs)::integer IN (6,7) THEN true 
                ELSE false 
            END as is_weekend
        FROM generate_series(start_date, end_date, '1 day'::interval) AS gs;
    END;
    $$ LANGUAGE plpgsql;
    SELECT generate_dim_date('2000-01-01', '2030-12-31');
    COMMIT;
    """

    print('Start processing...')

    with dwh_engine.connect() as conn:
        conn.execute(text(dwh_query))

    print('Processing completed.')
    return
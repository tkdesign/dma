from sqlalchemy import text

def load_dim_time(self, stage_engine, dwh_engine):
    if self is not None and self.is_aborted():
        print("Task aborted.")
        return

    dwh_query = """
    BEGIN;
    SET CONSTRAINTS ALL DEFERRED;
    CREATE OR REPLACE FUNCTION generate_dim_time_hours()
    RETURNS void AS $$
    BEGIN
        TRUNCATE TABLE public.dim_time CASCADE;
        INSERT INTO public.dim_time ("time", hour)
        SELECT 
             gs::time AS "time",
             EXTRACT(HOUR FROM gs)::integer AS hour
        FROM generate_series('2000-01-01 00:00:00'::timestamp, '2000-01-01 23:00:00'::timestamp, '1 hour'::interval) AS gs;
    END;
    $$ LANGUAGE plpgsql;
    SELECT generate_dim_time_hours();
    COMMIT;
    """

    print('Start processing...')

    with dwh_engine.connect() as conn:
        conn.execute(text(dwh_query))

    print('Processing completed.')
    return
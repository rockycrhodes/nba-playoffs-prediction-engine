-- Table: public.standings

-- DROP TABLE IF EXISTS public.standings;

CREATE TABLE IF NOT EXISTS public.standings
(
    season_id character varying COLLATE pg_catalog."default" NOT NULL,
    team_id integer NOT NULL,
    team_city text COLLATE pg_catalog."default",
    team_name text COLLATE pg_catalog."default",
    conference text COLLATE pg_catalog."default",
    conference_record text COLLATE pg_catalog."default",
    playoff_rank integer,
    clinch_indicator text COLLATE pg_catalog."default",
    division text COLLATE pg_catalog."default",
    division_record text COLLATE pg_catalog."default",
    division_rank integer,
    wins integer,
    losses integer,
    win_pct numeric,
    league_rank integer,
    record text COLLATE pg_catalog."default",
    run_date date NOT NULL,
    CONSTRAINT standings_pkey PRIMARY KEY (season_id, team_id, run_date)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.standings
    OWNER to postgres;